/// This script is responsible for sending the distributions calculated by the other script.
/// Funds belonging to accounts already closed, get sent back to the expired funds account.
///
/// NOTE: This script is not idempotent, meaning if it is ran multiple times it may over pay
/// some accounts. To safeguard against this
use clap::Parser;
use crossbeam::channel::{unbounded, Sender};
use futures::future::join_all;
use log::*;
use mev_claim_reconciler::{append_to_csv_file, read_csv_from_file, FlattenedDistribution};
use solana_program::hash::Hash;
use solana_program::pubkey::Pubkey;
use solana_program::system_instruction::transfer;
use solana_program::system_program;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::config::RpcSendTransactionConfig;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{EncodableKey, Keypair, Signer};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_transaction_status::UiTransactionEncoding;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON containing distribution amounts.
    #[arg(long, env)]
    distributions_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    jito_url: String,

    /// RPC to read data from.
    #[arg(long, env)]
    rpc_url: String,

    /// Path to file keeping track of completed distributions.
    #[arg(long, env)]
    completed_distributions_path: PathBuf,

    /// Path to file keeping track of distributions skipped due to closed accounts.
    #[arg(long, env)]
    skipped_distributions_path: PathBuf,

    /// Path to the wallet funding the distributions
    #[arg(long, env)]
    funding_wallet_path: PathBuf,
}

// Figment wants funds sent to a different account, so skip over them.
const FIGMENT_VOTE_ACCOUNT: &str = "26pV97Ce83ZQ6Kz9XT4td8tdoUFPTng8Fb8gPyc53dJx";

fn main() {
    env_logger::init();

    info!("Starting...");

    let args: Args = Args::parse();
    let runtime = Arc::new(Runtime::new().unwrap());
    let figment_vote_account = Pubkey::from_str(FIGMENT_VOTE_ACCOUNT).unwrap();

    let exit = Arc::new(AtomicBool::new(false));

    // Completed distribution channel
    let (completed_distributions_tx, completed_distributions_rx) =
        unbounded::<FlattenedDistribution>();

    let completed_distributions_path = args.completed_distributions_path.clone();
    let rx = completed_distributions_rx.clone();
    let rt = runtime.clone();
    let c_exit = exit.clone();
    ctrlc::set_handler(move || {
        info!("received exit signal");
        c_exit.store(true, Ordering::Relaxed);
        while let Ok(dist) = rx.recv() {
            if let Err(e) = append_to_csv_file(&dist, &completed_distributions_path) {
                error!(
                    "error writing distribution to file: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    dist.receiver, dist.amount_lamports, dist.tda_pubkey,
                );
            }
        }
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    let funder = Keypair::read_from_file(args.funding_wallet_path).unwrap();
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        args.rpc_url,
        CommitmentConfig::confirmed(),
    ));
    let jito_client = Arc::new(RpcClient::new_with_commitment(
        args.jito_url,
        CommitmentConfig::confirmed(),
    ));
    let rpc_version = runtime
        .block_on(rpc_client.get_version())
        .unwrap()
        .solana_core;
    info!("rpc server version: {}", rpc_version);

    println!("reading in distributions");
    let distributions: Vec<FlattenedDistribution> =
        read_csv_from_file(&args.distributions_path).unwrap();

    println!("reading completed distributions path");
    let completed_distributions: Vec<FlattenedDistribution> =
        read_csv_from_file(&args.completed_distributions_path).unwrap();

    let mut incomplete_distributions = Vec::new();
    for d in distributions {
        if completed_distributions
            .iter()
            .find(|c| c.receiver == d.receiver && c.tda_pubkey == d.tda_pubkey)
            .is_none()
            && d.receiver != figment_vote_account
        {
            incomplete_distributions.push(d);
        }
    }

    // Closed accounts don't get claimed for.
    info!("checking for closed accounts");
    let mut futs = Vec::with_capacity(incomplete_distributions.len());
    let semaphore = Arc::new(Semaphore::new(50));
    for d in incomplete_distributions {
        let c = rpc_client.clone();
        let s = semaphore.clone();
        let exit = exit.clone();
        futs.push(async move {
            const MAX_RETRIES: usize = 5;
            let mut retries = 0;
            loop {
                if exit.load(Ordering::Relaxed) {
                    return Ok((d, false));
                }
                let _permit = s.acquire().await.unwrap();
                let res = c.get_account(&d.receiver).await;
                drop(_permit);
                match res {
                    Ok(account) => {
                        return if account.data.len() == 0 && account.owner == system_program::id() {
                            Ok((d, false))
                        } else {
                            Ok((d, true))
                        };
                    }
                    Err(e) => {
                        match &e.kind {
                            ErrorKind::RpcError(e) => match e {
                                RpcError::ForUser(msg) => {
                                    if msg.contains("AccountNotFound") {
                                        return Ok((d, false));
                                    }
                                }
                                _ => {}
                            },
                            _ => {}
                        };
                        error!("error fetching account: {e:?}");
                        if retries >= MAX_RETRIES {
                            return Err(e);
                        }
                        retries += 1;
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
    }

    let mut incomplete_distributions = Vec::with_capacity(futs.len());
    let results = rt.block_on(join_all(futs));
    for maybe_result in results {
        let (d, should_distribute) = maybe_result.unwrap();
        if should_distribute {
            incomplete_distributions.push(d);
        } else {
            info!("account {} closed, skipping", d.receiver);
            append_to_csv_file(&d, &args.skipped_distributions_path).unwrap();
        }
    }

    if incomplete_distributions.is_empty() {
        info!("no distributions left");
        return;
    }

    let remaining_lamports: u64 = incomplete_distributions
        .iter()
        .map(|d| d.amount_lamports)
        .sum();
    info!("remaining lamports: {}", remaining_lamports);
    info!(
        "remaining distributions: {}",
        incomplete_distributions.len()
    );

    rt.spawn(async move {
        loop {
            send_transactions(
                &completed_distributions_tx,
                &jito_client,
                &rpc_client,
                &funder,
                &mut incomplete_distributions,
                &exit,
            )
            .await;

            warn!("Issues sending transactions, sleeping for a bit.");
            sleep(Duration::from_millis(5_000)).await;
            exit.store(true, Ordering::Relaxed);
        }
    });
    while let Ok(dist) = completed_distributions_rx.recv() {
        if let Err(e) = append_to_csv_file(&dist, &args.completed_distributions_path) {
            error!(
                    "error writing distribution to file: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    dist.receiver, dist.amount_lamports, dist.tda_pubkey,
                );
        }
    }
}

async fn send_transactions(
    completed_distributions_tx: &Sender<FlattenedDistribution>,
    jito_client: &Arc<RpcClient>,
    rpc_client: &Arc<RpcClient>,
    funder: &Keypair,
    incomplete_distributions: &mut Vec<FlattenedDistribution>,
    exit: &Arc<AtomicBool>,
) {
    let mut recent_blockhash = match rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
        .await
    {
        Ok((hash, _)) => RecentBlockhash {
            hash,
            last_updated: Instant::now(),
        },
        Err(e) => {
            error!("error fetching blockhash: {e:?}");
            return;
        }
    };

    const BATCH_SIZE: usize = 100;
    while !exit.load(Ordering::Relaxed) {
        let mut completed_distributions = Vec::with_capacity(incomplete_distributions.len());
        for batch in incomplete_distributions.chunks(BATCH_SIZE) {
            completed_distributions.extend(
                send_and_confirm_transactions(
                    batch,
                    &jito_client,
                    &rpc_client,
                    &funder,
                    &mut recent_blockhash,
                    &exit,
                )
                .await,
            );
            if exit.load(Ordering::Relaxed) {
                break;
            }
        }
        info!(
            "completed: {}, remaining: {}",
            completed_distributions.len(),
            incomplete_distributions.len() - completed_distributions.len()
        );
        // Only retain incomplete distributions
        incomplete_distributions
            .retain(|d| completed_distributions.iter().find(|c| c == &d).is_none());
        for c in completed_distributions {
            if let Err(e) = completed_distributions_tx.send(c) {
                error!(
                    "error sending distribution: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    c.receiver, c.amount_lamports, c.tda_pubkey,
                );
            }
        }
    }
}

#[derive(Copy, Clone)]
struct RecentBlockhash {
    hash: Hash,
    last_updated: Instant,
}

async fn send_and_confirm_transactions(
    batch: &[FlattenedDistribution],
    jito_client: &Arc<RpcClient>,
    rpc_client: &Arc<RpcClient>,
    funder: &Keypair,
    recent_blockhash: &mut RecentBlockhash,
    exit: &Arc<AtomicBool>,
) -> Vec<FlattenedDistribution> {
    if recent_blockhash.last_updated.elapsed() >= Duration::from_secs(30) {
        *recent_blockhash = match rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await
        {
            Ok((hash, _)) => RecentBlockhash {
                hash,
                last_updated: Instant::now(),
            },
            Err(e) => {
                println!("error fetching blockhash: {e:?}");
                exit.store(true, Ordering::Relaxed);
                return vec![];
            }
        };
    }
    let mut futs = Vec::with_capacity(batch.len());
    let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(10);
    for d in batch {
        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[
                priority_fee_ix.clone(),
                transfer(&funder.pubkey(), &d.receiver, d.amount_lamports),
            ],
            Some(&funder.pubkey()),
            &[funder],
            recent_blockhash.hash,
        ));
        let c_exit = exit.clone();
        let blockhash = recent_blockhash.hash;
        futs.push(async move {
            match jito_client.send_transaction(&tx).await {
                Ok(_) => loop {
                    match rpc_client.confirm_transaction(tx.get_signature()).await {
                        Ok(is_confirmed) => {
                            if is_confirmed {
                                return Some(*d);
                            } else {
                                match rpc_client
                                    .is_blockhash_valid(&blockhash, CommitmentConfig::confirmed())
                                    .await
                                {
                                    Ok(is_valid) => {
                                        if is_valid {
                                            sleep(Duration::from_millis(100)).await;
                                            continue;
                                        } else {
                                            return None;
                                        }
                                    }
                                    Err(e) => {
                                        error!("error checking blockhash: {e:?}");
                                        c_exit.store(true, Ordering::Relaxed);
                                        return None;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("error confirming tx: {e:?}");
                            c_exit.store(true, Ordering::Relaxed);
                            return None;
                        }
                    }
                },
                Err(e) => {
                    debug!(
                        "error confirming tx: {e:?}, b64 data: {}",
                        base64::encode(tx.message.serialize())
                    );
                    None
                }
            }
        });
    }

    let mut completed_distributions = Vec::with_capacity(batch.len());
    for res in join_all(futs).await {
        if let Some(d) = res {
            completed_distributions.push(d);
        }
    }

    completed_distributions
}
