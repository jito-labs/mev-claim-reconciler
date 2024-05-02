use csv::{Reader, Writer};
use jito_tip_distribution::state::Config;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use solana_program::clock::{Epoch, Slot};
use solana_program::hash::Hash;
use solana_program::pubkey::Pubkey;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::PathBuf;

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

#[derive(Clone, Deserialize, Serialize)]
pub struct TdaDistributions {
    #[serde(with = "pubkey_string_conversion")]
    pub tda_pubkey: Pubkey,
    #[serde(with = "pubkey_string_conversion")]
    pub validator_pubkey: Pubkey,

    /// Total amount that needs to get redistributed.
    pub total_remaining_lamports: u64,

    pub distributions: Vec<Distribution>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Distribution {
    #[serde(with = "pubkey_string_conversion")]
    pub receiver: Pubkey,
    pub amount_lamports: u64,
}

#[derive(Deserialize, Serialize)]
pub struct CompletedDistribution {
    #[serde(with = "pubkey_string_conversion")]
    pub receiver: Pubkey,
    #[serde(with = "pubkey_string_conversion")]
    pub tda: Pubkey,
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

pub fn read_json_from_file<T>(path: &PathBuf) -> serde_json::Result<T>
where
    T: DeserializeOwned,
{
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader)
}

#[derive(Deserialize, Serialize)]
pub struct FlattenedDistribution {
    #[serde(with = "pubkey_string_conversion")]
    pub tda_pubkey: Pubkey,
    #[serde(with = "pubkey_string_conversion")]
    pub receiver: Pubkey,
    pub amount_lamports: u64,
}

pub fn read_csv_from_file<T>(path: &PathBuf) -> csv::Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let mut reader = Reader::from_path(path).unwrap();
    let mut out: Vec<T> = vec![];
    let mut iter = reader.deserialize();
    while let Some(row) = iter.next() {
        out.push(row?);
    }
    Ok(out)
}
// pub fn read_csv_from_file_every_other_row<T>(path: &PathBuf) -> csv::Result<Vec<T>>
// where
//     T: DeserializeOwned,
// {
//     let mut reader = Reader::from_path(path).unwrap();
//     let mut out: Vec<T> = vec![];
//     let mut iter = reader.deserialize();
//     let mut i = 0;
//     while let Some(row) = iter.next() {
//         if i % 2 == 0 {
//             out.push(row?);
//         }
//         i += 1;
//     }
//     Ok(out)
// }

pub fn derive_config_account_address(tip_distribution_program_id: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id)
}

pub fn write_to_json_file(
    discrepancies: &[TdaDistributions],
    out_path: &PathBuf,
) -> io::Result<()> {
    let file = File::create(out_path)?;
    let mut writer = BufWriter::new(file);
    let json = serde_json::to_string_pretty(&discrepancies).unwrap();
    writer.write_all(json.as_bytes())?;
    writer.flush()?;

    Ok(())
}

pub fn append_to_csv_file(dist: &FlattenedDistribution, out_path: &PathBuf) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(out_path)
        .unwrap();
    let needs_headers = file.seek(io::SeekFrom::End(0))? == 0;
    let mut w = csv::WriterBuilder::new()
        .has_headers(needs_headers)
        .from_writer(file);
    w.serialize(dist)?;
    w.flush()
}

pub fn write_to_csv_file(
    tda_distributions: &[TdaDistributions],
    out_path: &PathBuf,
) -> io::Result<()> {
    let mut w = Writer::from_path(out_path)?;
    for d0 in tda_distributions {
        for d1 in &d0.distributions {
            w.serialize(FlattenedDistribution {
                tda_pubkey: d0.tda_pubkey,
                receiver: d1.receiver,
                amount_lamports: d1.amount_lamports,
            })?;
        }
    }
    w.flush()
}
