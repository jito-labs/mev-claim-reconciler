/// Issue occurred on 04/23/2024 where one warehouse node created an incorrect snapshot and uploaded
/// that as the root for some TDAs. The snapshot was incorrect because it only included ~91% of epoch
/// 603's slots ending at slot 260889752 as opposed to slot 260927999. This script is responsible for
/// figuring out which validators were affected by this and what difference in SOL was.
///
/// How it works:
///     1. Determine what validators were affected by comparing the correct snapshot root with what's on-chain.
///         a. if the root does not match their corresponding expected root, then assume the validator was affected
///     2. Calculate what needs to get distributed
///         a. subtract rent from total lamports in TDA, calculate validator commission
///         b. subtracting the above, calculate staker pro-rata distributions
///     3. Spit the distribution numbers out to a CSV
use anchor_lang::AccountDeserialize;
use clap::Parser;
use futures::future::join_all;
use jito_tip_distribution::state::{Config, TipDistributionAccount};
use mev_claim_reconciler::{
    Distribution, GeneratedMerkleTree, GeneratedMerkleTreeCollection, TdaDistributions, TreeNode,
};
use serde::de::DeserializeOwned;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing **incorrect** merkle root snapshot data.
    #[arg(long, env)]
    incorrect_snapshot_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    rpc_url: String,

    /// Path to write output to.
    #[arg(long, env)]
    out_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let tip_distribution_program: Pubkey =
        Pubkey::from_str("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7").unwrap();

    println!("Starting...");

    let args: Args = Args::parse();

    let rpc_client = Arc::new(RpcClient::new(args.rpc_url));
    println!(
        "rpc server version: {}",
        rpc_client.get_version().await.unwrap().solana_core
    );

    let (config_account, _) = derive_config_account_address(&tip_distribution_program);
    let account = rpc_client.get_account(&config_account).await.unwrap();
    let mut data = account.data.as_slice();
    let config: Config = Config::try_deserialize(&mut data).unwrap();
    println!("expired funds account: {}", config.expired_funds_account);

    println!("reading in snapshot files...");
    let incorrect_snapshot_path = args.incorrect_snapshot_path.clone();
    let incorrect_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(move || read_json_from_file(&incorrect_snapshot_path).unwrap())
            .await
            .unwrap();

    // Get affected validator TDAs.
    let affected_tda_cxs = get_affected_validator_tdas(incorrect_snapshot, &rpc_client).await;

    // Calc. rent
    let rent_exempt = rpc_client
        .get_minimum_balance_for_rent_exemption(TipDistributionAccount::SIZE)
        .await
        .unwrap();

    // Run the calcs
    let distributions = calc_distributions(affected_tda_cxs, rent_exempt);

    println!(
        "Total funds remaining: {}",
        distributions
            .iter()
            .map(|d| d.total_remaining_lamports)
            .sum::<u64>()
    );

    println!("writing to file...");

    let out_path = args.out_path;
    spawn_blocking(move || write_to_json_file(&distributions, &out_path).unwrap())
        .await
        .unwrap();

    println!("done...");
}

struct TdaContext {
    /// The tda itself
    tda: TipDistributionAccount,
    /// The amount of lamports in the account
    lamports: u64,
    /// The tda's pubkey
    pubkey: Pubkey,
    /// Snapshot of the validator's merkle tree.
    incorrect_snapshot: GeneratedMerkleTree,
}

async fn get_affected_validator_tdas(
    incorrect_snapshot: GeneratedMerkleTreeCollection,
    rpc_client: &Arc<RpcClient>,
) -> Vec<TdaContext> {
    // Get all tip distribution accounts
    println!("fetching tip distribution accounts");
    let mut futs = Vec::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for tree in incorrect_snapshot.generated_merkle_trees {
        let c = rpc_client.clone();
        futs.push(async move {
            const MAX_RETRIES: usize = 10;
            let mut retry_count = 0;

            let tda_key = tree.tip_distribution_account;
            let incorrect_merkle_root = tree.merkle_root.to_bytes();

            loop {
                let resp = c.get_account(&tda_key).await;
                if resp.is_ok() {
                    let account = resp.unwrap();
                    let mut data = account.data.as_slice();
                    let tda: TipDistributionAccount =
                        TipDistributionAccount::try_deserialize(&mut data)
                            .expect("failed to deserialize tip_distribution_account state");
                    return if tda.merkle_root.as_ref().unwrap().root == incorrect_merkle_root {
                        Ok(Some(TdaContext {
                            tda,
                            lamports: account.lamports,
                            pubkey: tda_key,
                            incorrect_snapshot: tree,
                        }))
                    } else {
                        Ok(None)
                    };
                }
                if retry_count >= MAX_RETRIES {
                    return Err(resp.err().unwrap());
                }
                retry_count += 1;
                sleep(Duration::from_millis(500)).await;
            }
        });
    }

    let mut tdas = Vec::with_capacity(futs.len());
    for maybe_tda_context in join_all(futs).await {
        let tda_context = maybe_tda_context.unwrap();
        if let Some(tda_context) = tda_context {
            tdas.push(tda_context);
        }
    }
    tdas
}

fn calc_distributions(tda_cxs: Vec<TdaContext>, rent_exempt: u64) -> Vec<TdaDistributions> {
    let mut tda_distributions = Vec::with_capacity(tda_cxs.len());
    for cx in tda_cxs {
        // Total pot available
        let total_pot = cx.lamports - rent_exempt;

        // Validator's funds
        let validator_amount = (total_pot as u128)
            .checked_mul(cx.tda.validator_commission_bps as u128)
            .unwrap()
            .checked_div(10_000)
            .unwrap() as u64;

        // Subtract validator from total_pot
        let staker_pot = total_pot - validator_amount;

        // Get all tree nodes that are not the validator
        let staker_nodes: Vec<&TreeNode> = cx
            .incorrect_snapshot
            .tree_nodes
            .iter()
            .filter(|n| n.claimant != cx.tda.validator_vote_account)
            .collect();

        let mut staker_amounts = HashMap::with_capacity(staker_nodes.len());
        for n in staker_nodes {
            // The max that was claimed (this is the incorrect number derived by the incorrect snapshot and what's uploaded on-chain).
            // We can use what was uploaded on-chain to figure out the pro-rate distributions. For example:
            //                    (incorrect_amount_claimed_by_staker)                    X
            //    staker_amount = ------------------------------------ = ------------------------------------
            //                        (incorrect_max_total_claim)       (remaining_funds_in_tda - rent_exempt - validator_commission)
            let amount = (n.amount as u128 * staker_pot as u128)
                / cx.incorrect_snapshot.max_total_claim as u128;
            staker_amounts.insert(n.claimant, amount as u64);
        }

        let mut distributions = Vec::with_capacity(cx.incorrect_snapshot.tree_nodes.len());
        for (receiver, amount_lamports) in staker_amounts {
            distributions.push(Distribution {
                receiver,
                amount_lamports,
            });
        }
        distributions.push(Distribution {
            receiver: cx.tda.validator_vote_account,
            amount_lamports: validator_amount,
        });

        tda_distributions.push(TdaDistributions {
            validator_pubkey: cx.tda.validator_vote_account,
            total_remaining_lamports: total_pot,
            distributions,
        });
    }
    tda_distributions
}
