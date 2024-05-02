/// This script is responsible for sending the distributions calculated by the other script.
/// Funds belonging to accounts already closed, get sent back to the expired funds account.
///
/// NOTE: This script is not idempotent, meaning if it is ran multiple times it may over pay
/// some accounts. To safeguard against this
use clap::Parser;
use crossbeam::channel::{unbounded, Sender};
use futures::future::join_all;
use mev_claim_reconciler::{append_to_csv_file, read_csv_from_file, FlattenedDistribution};
use solana_program::hash::Hash;
use solana_program::system_instruction::transfer;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{EncodableKey, Keypair, Signer};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON containing distribution amounts.
    #[arg(long, env)]
    distributions_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    rpc_url: String,

    /// Path to file keeping track of completed distributions.
    #[arg(long, env)]
    completed_distributions_path: PathBuf,

    /// Path to the wallet funding the distributions. If this is not supplied, then this is effectively running in dry-mode.
    #[arg(long, env)]
    funding_wallet_path: Option<PathBuf>,
}

fn main() {
    println!("Starting...");

    let args: Args = Args::parse();
    let runtime = Arc::new(Runtime::new().unwrap());

    let exit = Arc::new(AtomicBool::new(false));

    // Completed distribution channel
    let (completed_distributions_tx, completed_distributions_rx) =
        unbounded::<FlattenedDistribution>();

    let completed_distributions_path = args.completed_distributions_path.clone();
    let rx = completed_distributions_rx.clone();
    let rt = runtime.clone();
    let c_exit = exit.clone();
    ctrlc::set_handler(move || {
        println!("received exit signal");
        c_exit.store(true, Ordering::Relaxed);
        while let Ok(dist) = rx.recv() {
            if let Err(e) = append_to_csv_file(&dist, &completed_distributions_path) {
                println!(
                    "error writing distribution to file: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    dist.receiver, dist.amount_lamports, dist.tda_pubkey,
                );
            }
        }
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    let maybe_rpc_client_and_keypair = if let Some(keypair_path) = &args.funding_wallet_path {
        println!("Keypair path supplied, reading in key.");
        let keypair = Keypair::read_from_file(keypair_path).unwrap();
        let rpc_client = Arc::new(RpcClient::new(args.rpc_url));
        let rpc_version = runtime
            .block_on(rpc_client.get_version())
            .unwrap()
            .solana_core;
        println!("rpc server version: {}", rpc_version);
        Some((rpc_client, keypair))
    } else {
        println!("No keypair supplied, running in dry run mode.");
        None
    };

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
        {
            incomplete_distributions.push(d);
        }
    }

    let remaining_lamports: u64 = incomplete_distributions
        .iter()
        .map(|d| d.amount_lamports)
        .sum();
    println!("remaining lamports: {}", remaining_lamports);
    println!(
        "remaining distributions: {}",
        incomplete_distributions.len()
    );

    if let Some((rpc_client, funder)) = maybe_rpc_client_and_keypair {
        let _send_task = rt.spawn(send_transactions(
            completed_distributions_tx,
            rpc_client,
            funder,
            incomplete_distributions,
            exit,
        ));
        while let Ok(dist) = completed_distributions_rx.recv() {
            if let Err(e) = append_to_csv_file(&dist, &args.completed_distributions_path) {
                println!(
                    "error writing distribution to file: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    dist.receiver, dist.amount_lamports, dist.tda_pubkey,
                );
            }
        }
    }
}

async fn send_transactions(
    completed_distributions_tx: Sender<FlattenedDistribution>,
    rpc_client: Arc<RpcClient>,
    funder: Keypair,
    mut incomplete_distributions: Vec<FlattenedDistribution>,
    exit: Arc<AtomicBool>,
) {
    let mut recent_blockhash = match rpc_client.get_latest_blockhash().await {
        Ok(hash) => RecentBlockhash {
            hash,
            last_updated: Instant::now(),
        },
        Err(e) => {
            println!("error fetching blockhash: {e:?}");
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
        println!(
            "completed: {}, remaining: {}",
            completed_distributions.len(),
            incomplete_distributions.len() - completed_distributions.len()
        );
        // Only retain incomplete distributions
        incomplete_distributions
            .retain(|d| completed_distributions.iter().find(|c| c == &d).is_none());
        for c in completed_distributions {
            if let Err(e) = completed_distributions_tx.send(c) {
                println!(
                    "error sending distribution: [receiver={}, amount_lamports={}, tda_pubkey={}, error={e:?}]",
                    c.receiver, c.amount_lamports, c.tda_pubkey,
                );
            }
        }
    }
}

struct RecentBlockhash {
    hash: Hash,
    last_updated: Instant,
}

async fn send_and_confirm_transactions(
    batch: &[FlattenedDistribution],
    rpc_client: &Arc<RpcClient>,
    funder: &Keypair,
    recent_blockhash: &mut RecentBlockhash,
    exit: &Arc<AtomicBool>,
) -> Vec<FlattenedDistribution> {
    if recent_blockhash.last_updated.elapsed() >= Duration::from_secs(30) {
        *recent_blockhash = match rpc_client.get_latest_blockhash().await {
            Ok(hash) => RecentBlockhash {
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
    for d in batch {
        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[transfer(&funder.pubkey(), &d.receiver, d.amount_lamports)],
            Some(&funder.pubkey()),
            &[funder],
            recent_blockhash.hash,
        ));
        futs.push(async move {
            if let Ok(_sig) = rpc_client.send_and_confirm_transaction(&tx).await {
                Some(*d)
            } else {
                None
            }
        });
    }

    let mut completed_distributions = Vec::with_capacity(batch.len());
    for res in join_all(futs).await {
        if let Some(d) = res {
            completed_distributions.push(d);
        }
    }
    println!("completed {} distributions", completed_distributions.len());

    completed_distributions
}
