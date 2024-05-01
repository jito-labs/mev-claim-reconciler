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
    derive_config_account_address, read_json_from_file, write_to_csv_file, write_to_json_file,
    Distribution, GeneratedMerkleTree, GeneratedMerkleTreeCollection, TdaDistributions, TreeNode,
};
use solana_program::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
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

    /// Path to JSON file containing the **correct** merkle root snapshot data.
    #[arg(long, env)]
    correct_snapshot_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    rpc_url: String,

    /// Path to write JSON output to.
    #[arg(long, env)]
    json_out_path: PathBuf,

    /// Path to write CSV output to.
    #[arg(long, env)]
    csv_out_path: PathBuf,
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
    let correct_snapshot_path = args.correct_snapshot_path.clone();
    let correct_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(move || read_json_from_file(&correct_snapshot_path).unwrap())
            .await
            .unwrap();
    let incorrect_snapshot_path = args.incorrect_snapshot_path.clone();
    let incorrect_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(move || read_json_from_file(&incorrect_snapshot_path).unwrap())
            .await
            .unwrap();

    // Get affected validator TDAs.
    println!("getting affected validator tdas");
    let affected_tda_cxs =
        get_affected_validator_tdas(correct_snapshot, incorrect_snapshot, &rpc_client).await;

    // Run the calcs
    let distributions = calc_distributions(affected_tda_cxs.into_values().collect());

    println!(
        "Total funds remaining: {}",
        distributions
            .iter()
            .map(|d| d.total_remaining_lamports)
            .sum::<u64>()
    );

    println!("writing to file...");

    let json_out_path = args.json_out_path;
    let _distributions = distributions.clone();
    spawn_blocking(move || write_to_json_file(&distributions, &json_out_path).unwrap())
        .await
        .unwrap();

    let csv_out_path = args.csv_out_path;
    spawn_blocking(move || write_to_csv_file(&_distributions, &csv_out_path).unwrap())
        .await
        .unwrap();

    println!("done...");
}

struct TdaContext {
    /// The tda itself, fetched from the chain.
    tda: TipDistributionAccount,
    /// Snapshot of the validator's merkle tree, this is what was uploaded to the chain.
    incorrect_snapshot: GeneratedMerkleTree,
    /// Snapshot of the validator's correct merkle tree, this is what should have been uploaded to the chain.
    correct_snapshot: GeneratedMerkleTree,
    tda_pubkey: Pubkey,
}

async fn get_affected_validator_tdas(
    correct_snapshot: GeneratedMerkleTreeCollection,
    incorrect_snapshot: GeneratedMerkleTreeCollection,
    rpc_client: &Arc<RpcClient>,
) -> HashMap<Pubkey /* tda pda */, TdaContext> {
    println!("fetching tip distribution accounts");

    let mut correct_trees: HashMap<Pubkey, GeneratedMerkleTree> = correct_snapshot
        .generated_merkle_trees
        .into_iter()
        .map(|t| (t.tip_distribution_account, t))
        .collect();
    let mut futs = Vec::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for incorrect_tree in incorrect_snapshot.generated_merkle_trees {
        let c = rpc_client.clone();
        let correct_tree = correct_trees
            .remove(&incorrect_tree.tip_distribution_account)
            .unwrap();
        futs.push(async move {
            const MAX_RETRIES: usize = 10;
            let mut retry_count = 0;

            let tda_pubkey = incorrect_tree.tip_distribution_account;
            let incorrect_merkle_root = incorrect_tree.merkle_root.to_bytes();

            loop {
                let resp = c.get_account(&tda_pubkey).await;
                if resp.is_ok() {
                    let account = resp.unwrap();
                    let mut data = account.data.as_slice();
                    let tda: TipDistributionAccount =
                        TipDistributionAccount::try_deserialize(&mut data)
                            .expect("failed to deserialize tip_distribution_account state");
                    return if tda.merkle_root.as_ref().unwrap().root == incorrect_merkle_root {
                        Ok(Some(TdaContext {
                            tda_pubkey,
                            tda,
                            incorrect_snapshot: incorrect_tree,
                            correct_snapshot: correct_tree,
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

    let mut tdas = HashMap::with_capacity(futs.len());
    for maybe_tda_context in join_all(futs).await {
        let tda_context = maybe_tda_context.unwrap();
        if let Some(tda_context) = tda_context {
            tdas.insert(tda_context.tda_pubkey, tda_context);
        }
    }
    tdas
}

fn calc_distributions(tda_cxs: Vec<TdaContext>) -> Vec<TdaDistributions> {
    let mut tda_distributions = Vec::with_capacity(tda_cxs.len());
    for cx in tda_cxs {
        // Total pot remaining
        let max_claim_snapshot_diff = cx
            .correct_snapshot
            .max_total_claim
            .checked_sub(cx.incorrect_snapshot.max_total_claim)
            .unwrap();

        // If it's 0 then no new MEV was collected for this validator
        if max_claim_snapshot_diff == 0 {
            continue;
        }

        // Validator's diff owed
        let validator_amount_owed = (max_claim_snapshot_diff as u128)
            .checked_mul(cx.tda.validator_commission_bps as u128)
            .unwrap()
            .checked_div(10_000)
            .unwrap() as u64;

        // Subtract validator from total available
        let total_staker_amount_owed = max_claim_snapshot_diff
            .checked_sub(validator_amount_owed)
            .unwrap();

        // Get all tree nodes that are not the validator
        let mut validator_claimed_amount = 0;
        let staker_nodes: Vec<&TreeNode> = cx
            .correct_snapshot
            .tree_nodes
            .iter()
            .filter(|n| {
                if n.claimant != cx.tda.validator_vote_account {
                    true
                } else {
                    validator_claimed_amount = n.amount;
                    false
                }
            })
            .collect();

        // The expected validator commission.
        let expected_validator_amount = (cx.correct_snapshot.max_total_claim as u128)
            .checked_mul(cx.tda.validator_commission_bps as u128)
            .unwrap()
            .checked_div(10_000)
            .unwrap() as u64;
        let staker_max_claimed = (cx
            .correct_snapshot
            .max_total_claim
            .checked_sub(expected_validator_amount))
        .unwrap() as u128;

        let mut staker_amounts = HashMap::with_capacity(staker_nodes.len());
        if staker_max_claimed > 0 {
            for n in staker_nodes {
                // The max that was claimed (this is the incorrect number derived by the incorrect snapshot and what's uploaded on-chain).
                // We can use what was uploaded on-chain to figure out the pro-rate distributions. For example:
                // staker_amount = (correct_staker_claim_amount)/(correct_max_total_claim - validator_commission) = X/(max_claim_snap_diff - validator_commission)
                let amount = (n.amount as u128)
                    .checked_mul(total_staker_amount_owed as u128)
                    .unwrap()
                    .checked_div(staker_max_claimed)
                    .unwrap();
                staker_amounts.insert(n.claimant, amount as u64);
            }
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
            amount_lamports: validator_amount_owed,
        });

        tda_distributions.push(TdaDistributions {
            tda_pubkey: cx.tda_pubkey,
            validator_pubkey: cx.tda.validator_vote_account,
            total_remaining_lamports: max_claim_snapshot_diff,
            distributions,
        });
    }
    tda_distributions
}
