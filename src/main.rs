use clap::Parser;
use futures::future::join_all;
use jito_tip_distribution::state::TipDistributionAccount;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use solana_program::clock::{Epoch, Slot};
use solana_program::hash::Hash;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::task::spawn_blocking;

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

struct DiscrepancyOutputColl {
    num_validators_affected: usize,
    /// abs(incorrect snapshot total - incorrect snapshot total)
    total_sol_diff: u64,
    discrepancies: Vec<DiscrepancyOutput>,
}

#[derive(Deserialize, Serialize)]
struct DiscrepancyOutput {
    #[serde(with = "pubkey_string_conversion")]
    validator_pubkey: Pubkey,
    /// Correct snapshot total - incorrect snapshot total
    sol_diff: i64,
}

#[tokio::main]
async fn main() {
    const TIP_DISTRIBUTION_PROGRAM: Pubkey =
        Pubkey::from_str("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7").unwrap();

    println!("Starting...");
    let args: Args = Args::parse();

    println!("reading in snapshot files...");
    let incorrect_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(read_json_from_file(&args.incorrect_snapshot_path).unwrap())
            .await
            .unwrap();
    let correct_snapshot: GeneratedMerkleTreeCollection =
        spawn_blocking(read_json_from_file(&args.correct_snapshot_path).unwrap())
            .await
            .unwrap();

    // Create map of TDA pubkey to merkle root for easy lookup later
    let mut correct_roots = HashMap::with_capacity(correct_snapshot.generated_merkle_trees.len());
    let mut incorrect_roots =
        HashMap::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for tree in &incorrect_snapshot.generated_merkle_trees {
        incorrect_roots.insert(tree.tip_distribution_account, tree);
    }
    for tree in &correct_snapshot.generated_merkle_trees {
        correct_roots.insert(tree.tip_distribution_account, tree);
    }

    // Make sure both snapshots have exactly the same TDAs.
    assert_eq!(
        correct_roots.keys().collect::<HashSet<_>>(),
        incorrect_roots.keys().collect::<HashSet<_>>(),
        "snapshots contain different tip distribution accounts"
    );

    let rpc_client = RpcClient::new(args.rpc_url);
    println!(
        "rpc server version: {}",
        rpc_client.get_version().await.unwrap().solana_core
    );

    // Get all tip distribution accounts
    println!("fetching tip distribution accounts");
    let mut futs = Vec::with_capacity(incorrect_snapshot.generated_merkle_trees.len());
    for tree in incorrect_snapshot.generated_merkle_trees {
        futs.push(async {
            let account = rpc_client
                .get_account(&tree.tip_distribution_account)
                .await?;
            (tree.tip_distribution_account, account)
        });
    }

    // Deserialize accounts
    println!("deserializing tip distribution accounts");
    let mut onchain_tdas: Vec<(Pubkey, TipDistributionAccount)> = Vec::with_capacity(futs.len());
    for maybe_account in join_all(futs).await {
        let (pk, account) = maybe_account.unwrap();
        let mut data = account.data.as_slice();
        onchain_tdas.push((
            pk,
            TipDistributionAccount::try_deserialize(&mut data)
                .expect("failed to deserialize tip_distribution_account state"),
        ));
    }

    let mut discrepancies = Vec::new();
    let mut total_sol_diff = 0u64;
    // Check the merkle roots against the snapshots
    for (pk, tda) in onchain_tdas {
        let actual_root = tda.merkle_root.unwrap();
        let correct_tda = correct_roots.get(&pk).unwrap();
        let incorrect_tda = incorrect_roots.get(&pk).unwrap();

        if actual_root.root == correct_tda.merkle_root.to_bytes() {
            continue;
        }
        if actual_root.root != incorrect_tda.merkle_root.to_bytes() {
            panic!(
                "account {} does not contain a root matching either snapshot",
                pk
            );
        }

        let sol_diff = correct_tda.max_total_claim as i64 - incorrect_tda.max_total_claim as i64;
        discrepancies.push(DiscrepancyOutput {
            validator_pubkey: tda.validator_vote_account,
            sol_diff,
        });
        total_sol_diff += sol_diff.abs();
    }
    let output = DiscrepancyOutputColl {
        num_validators_affected: discrepancies.len(),
        total_sol_diff,
        discrepancies,
    };

    println!("writing discrepancies...");

    spawn_blocking(write_to_json_file(output, &args.out_path).unwrap())
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
    use solana_program::pubkey::Pubkey;
    use {
        serde::{self, Deserialize, Deserializer, Serializer},
        std::str::FromStr,
    };

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

fn write_to_json_file(
    data: &DiscrepancyOutputColl,
    file_path: &PathBuf,
) -> Result<(), MerkleRootGeneratorError> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);
    let json = serde_json::to_string_pretty(&data).unwrap();
    writer.write_all(json.as_bytes())?;
    writer.flush()?;

    Ok(())
}
