/// This script is responsible for sending the distributions calculated by the other script.
/// Funds belonging to accounts already closed, get sent back to the expired funds account.
///
/// NOTE: This script is not idempotent, meaning if it is ran multiple times it may over pay
/// some accounts. To safeguard against this
use clap::Parser;
use mev_claim_reconciler::{
    append_to_csv_file, read_csv_from_file, CompletedDistribution, FlattenedDistribution,
    TdaDistributions,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{EncodableKey, Keypair};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::spawn_blocking;

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

#[tokio::main]
async fn main() {
    println!("Starting...");

    let args: Args = Args::parse();

    let maybe_rpc_client_and_keypair = if let Some(keypair_path) = &args.funding_wallet_path {
        println!("Keypair path supplied, reading in key.");
        let keypair = Keypair::read_from_file(keypair_path).unwrap();
        let rpc_client = Arc::new(RpcClient::new(args.rpc_url));
        println!(
            "rpc server version: {}",
            rpc_client.get_version().await.unwrap().solana_core
        );
        Some((rpc_client, keypair))
    } else {
        println!("No keypair supplied, running in dry run mode.");
        None
    };

    println!("reading in distributions");
    let distributions_path = args.distributions_path.clone();
    let distributions: Vec<FlattenedDistribution> =
        spawn_blocking(move || read_csv_from_file(&distributions_path).unwrap())
            .await
            .unwrap();

    println!("reading completed distributions path");
    let completed_distributions_path = args.completed_distributions_path.clone();
    let completed_distributions: Vec<FlattenedDistribution> =
        spawn_blocking(move || read_csv_from_file(&completed_distributions_path).unwrap())
            .await
            .unwrap();

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
        send_transactions(
            rpc_client,
            funder,
            args.completed_distributions_path,
            completed_distributions,
        )
        .await;
    }
}

async fn send_transactions(
    rpc_client: Arc<RpcClient>,
    funder: Keypair,
    completed_distributions_path: PathBuf,
    completed_distributions: Vec<FlattenedDistribution>,
) {
    let mut c = 0;
    while c < 10 {
        append_to_csv_file(
            &FlattenedDistribution {
                tda_pubkey: Default::default(),
                receiver: Default::default(),
                amount_lamports: c,
            },
            &completed_distributions_path,
        )
        .expect("TODO: panic message");
        c += 1;
    }
}
