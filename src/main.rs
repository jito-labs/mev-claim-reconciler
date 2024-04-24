use anchor_lang::AccountDeserialize;
use clap::Parser;
use csv::Writer;
use futures::future::join_all;
use jito_tip_distribution::state::{Config, TipDistributionAccount};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use solana_program::clock::{Epoch, Slot};
use solana_program::hash::Hash;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

/// Issue occurred on 04/23/2024 where one warehouse node created an incorrect snapshot and uploaded
/// that as the root for some TDAs. The snapshot was incorrect because it only included ~91% of epoch
/// 603's slots ending at slot 260889752 as opposed to slot 260927999. This script is responsible for
/// figuring out which validators were affected by this and what difference in SOL was.
///
/// How it works:
///     Given the two snapshots look at each TDA for epoch 603 and determine which TDAs' roots match
///     that of the wrong snapshot. For all of those TDAs write the affected validators and the missing
///     SOL amount to an JSON file.

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing **incorrect** merkle root snapshot data.
    #[arg(long, env)]
    incorrect_snapshot_path: PathBuf,

    /// Path to JSON file containing **correct** merkle root snapshot data.
    #[arg(long, env)]
    correct_snapshot_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    rpc_url: String,

    /// Path write discrepancies to.
    #[arg(long, env)]
    out_path: PathBuf,
}

#[derive(Deserialize, Serialize)]
pub struct GeneratedMerkleTreeCollection {
    pub generated_merkle_trees: Vec<GeneratedMerkleTree>,
    pub bank_hash: String,
    pub epoch: Epoch,
    pub slot: Slot,
}

#[derive(Deserialize, Serialize)]
pub struct GeneratedMerkleTree {
    #[serde(with = "pubkey_string_conversion")]
    pub tip_distribution_account: Pubkey,
    #[serde(with = "pubkey_string_conversion")]
    pub merkle_root_upload_authority: Pubkey,
    pub merkle_root: Hash,
    pub tree_nodes: Vec<TreeNode>,
    pub max_total_claim: u64,
    pub max_num_nodes: u64,
}

#[derive(Deserialize, Serialize)]
pub struct TreeNode {
    /// The stake account entitled to redeem.
    #[serde(with = "pubkey_string_conversion")]
    pub claimant: Pubkey,

    /// Pubkey of the ClaimStatus PDA account, this account should be closed to reclaim rent.
    #[serde(with = "pubkey_string_conversion")]
    pub claim_status_pubkey: Pubkey,

    /// Bump of the ClaimStatus PDA account
    pub claim_status_bump: u8,

    #[serde(with = "pubkey_string_conversion")]
    pub staker_pubkey: Pubkey,

    #[serde(with = "pubkey_string_conversion")]
    pub withdrawer_pubkey: Pubkey,

    /// The amount this account is entitled to.
    pub amount: u64,

    /// The proof associated with this TreeNode
    pub proof: Vec<[u8; 32]>,
}

#[derive(Deserialize, Serialize)]
struct DiscrepancyOutput {
    #[serde(with = "pubkey_string_conversion")]
    validator_pubkey: Pubkey,

    /// Correct snapshot total - incorrect snapshot total, this is only stakers not validator funds
    staker_lamports_diff: i64,

    /// Correct snapshot validator funds - incorrect snapshot validator funds, this is only the validator diff not stakers.
    validator_lamports_diff: i64,
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
    let correct_snapshot_path = args.correct_snapshot_path.clone();
    let correct_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(move || read_json_from_file(&correct_snapshot_path).unwrap())
            .await
            .unwrap();

    // Create map of TDA pubkey to merkle root for easy lookup later
    let mut correct_roots: HashMap<Pubkey, &GeneratedMerkleTree> =
        HashMap::with_capacity(correct_snapshot.generated_merkle_trees.len());
    let mut incorrect_roots: HashMap<Pubkey, &GeneratedMerkleTree> =
        HashMap::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for tree in &incorrect_snapshot.generated_merkle_trees {
        incorrect_roots.insert(tree.tip_distribution_account, tree);
    }
    for tree in &correct_snapshot.generated_merkle_trees {
        correct_roots.insert(tree.tip_distribution_account, tree);
    }

    // Get all tip distribution accounts
    println!("fetching tip distribution accounts");
    let mut futs = Vec::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for tree in &correct_snapshot.generated_merkle_trees {
        let tda = tree.tip_distribution_account;
        let c = rpc_client.clone();
        futs.push(async move {
            const MAX_RETRIES: usize = 10;
            let mut retry_count = 0;
            loop {
                let account = c.get_account(&tda).await;
                if account.is_ok() {
                    return (tda, account);
                }
                if retry_count >= MAX_RETRIES {
                    return (tda, account);
                }
                retry_count += 1;
                sleep(Duration::from_millis(500)).await;
            }
        });
    }

    // Deserialize accounts
    println!("deserializing tip distribution accounts");
    let mut onchain_tdas: Vec<(Pubkey, TipDistributionAccount)> = Vec::with_capacity(futs.len());
    for (pk, maybe_account) in join_all(futs).await {
        let account = maybe_account.unwrap();
        let mut data = account.data.as_slice();
        onchain_tdas.push((
            pk,
            TipDistributionAccount::try_deserialize(&mut data)
                .expect("failed to deserialize tip_distribution_account state"),
        ));
    }

    let mut discrepancies = Vec::new();
    // Check the merkle roots against the snapshots
    for (pk, tda) in onchain_tdas {
        let actual_root = tda.merkle_root.unwrap();

        let Some(correct_tda) = correct_roots.get(&pk) else {
            panic!("missing {pk} from correct snapshot");
        };

        if correct_tda.merkle_root.to_bytes() == actual_root.root {
            continue;
        }

        let correct_validator_amount = (correct_tda.max_total_claim as u128)
            .checked_mul(tda.validator_commission_bps as u128)
            .unwrap()
            .checked_div(10_000)
            .unwrap() as i64;
        let correct_staker_amount =
            correct_tda.max_total_claim as i64 - correct_validator_amount as i64;

        let (validator_lamports_diff, staker_lamports_diff) = match incorrect_roots.get(&pk) {
            Some(incorrect_tda) => {
                if actual_root.root != incorrect_tda.merkle_root.to_bytes() {
                    panic!(
                        "account {} does not contain a root matching either snapshot",
                        pk
                    );
                }

                // Calc. incorrect amounts.
                let correct_validator_amount = (correct_tda.max_total_claim as u128)
                    .checked_mul(tda.validator_commission_bps as u128)
                    .unwrap()
                    .checked_div(10_000)
                    .unwrap() as i64;
                let incorrect_validator_amount = (incorrect_tda.max_total_claim as u128)
                    .checked_mul(tda.validator_commission_bps as u128)
                    .unwrap()
                    .checked_div(10_000)
                    .unwrap() as i64;
                let incorrect_staker_amount =
                    incorrect_tda.max_total_claim as i64 - incorrect_validator_amount as i64;

                // Subtract validator's commission
                (
                    correct_validator_amount - incorrect_validator_amount,
                    correct_staker_amount - incorrect_staker_amount,
                )
            }
            // If not in the incorrect snapshot, then the validator probably came on later in the epoch after the incorrect snapshot was created.
            // Therefore we need to reimburse the entire thing from the correct amount.
            None => {
                println!("missing {pk} from incorrect snapshot");
                (correct_validator_amount, correct_staker_amount)
            }
        };

        discrepancies.push(DiscrepancyOutput {
            validator_pubkey: tda.validator_vote_account,
            staker_lamports_diff,
            validator_lamports_diff,
        });
    }

    println!("writing discrepancies...");

    let out_path = args.out_path;
    spawn_blocking(move || write_to_csv(&discrepancies, &out_path).unwrap())
        .await
        .unwrap();

    println!("done...");
}

pub fn read_json_from_file<T>(path: &PathBuf) -> serde_json::Result<T>
where
    T: DeserializeOwned,
{
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader)
}

mod pubkey_string_conversion {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use solana_program::pubkey::Pubkey;
    use std::str::FromStr;

    pub(crate) fn serialize<S>(pubkey: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&pubkey.to_string())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Pubkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn derive_config_account_address(tip_distribution_program_id: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id)
}

fn write_to_csv(discrepancies: &[DiscrepancyOutput], out_path: &PathBuf) -> io::Result<()> {
    let mut w = Writer::from_path(out_path)?;
    for d in discrepancies {
        w.serialize(d)?;
    }
    w.flush()
}
