use std::path::PathBuf;

/// This script is responsible for sending the distributions calculated by the other script.
/// Funds belonging to accounts already closed, get sent back to the expired funds account.
///
/// NOTE: This script is not idempotent, meaning if it is ran multiple times it may over pay
/// some accounts. To safeguard against this

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON containing distribution amounts.
    #[arg(long, env)]
    distributions_path: PathBuf,

    /// RPC to send transactions through.
    #[arg(long, env)]
    rpc_url: String,

    /// Path to mark distributions as complete.
    #[arg(long, env)]
    out_path: PathBuf,

    /// Path to mark distributions as complete.
    #[arg(long, env)]
    completed_distributions_path: PathBuf,
}

fn main() {}
