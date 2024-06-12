use super::subcommands::*;
use crate::TablCliError;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub(crate) async fn run_cli() -> Result<(), TablCliError> {
    match Cli::parse().command {
        Commands::Ls(args) => ls_command(args).await,
        Commands::Schema(args) => schema_command(args),
        Commands::Stats(args) => stats_command(args),
        Commands::Cast(args) => cast_command(args),
        Commands::Drop(args) => drop_command(args),
        Commands::Merge(args) => merge_command(args),
        Commands::Partition(args) => partition_command(args),
        Commands::Pl(args) => pl_command(args),
    }
}

/// Utility for creating and managing MESC RPC configurations
#[derive(Parser)]
#[clap(author, version, about, long_about = None, disable_help_subcommand = true)]
pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) command: Commands,
}

/// Define your subcommands as an enum
#[derive(Subcommand)]
#[command()]
pub(crate) enum Commands {
    //
    // // read commands
    //
    /// Show list of tabular files
    Ls(LsArgs),
    /// Show schema of tabular files
    Schema(SchemaArgs),
    /// Show stats of tabular files
    Stats(StatsArgs),
    //
    // // edit commands
    //
    /// Cast columns of tabular files
    Cast(CastArgs),
    /// Drop columns from tabular files
    Drop(DropArgs),
    /// Merge tabular files
    Merge(MergeArgs),
    /// Partition tabular files
    Partition(PartitionArgs),
    /// Edit files using polars python syntax
    Pl(PlArgs),
}

//
// // read commands
//

/// Arguments for the `ls` subcommand
#[derive(Parser)]
pub(crate) struct LsArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(long)]
    pub(crate) tree: bool,

    /// number of file names to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// show absolute paths instead of relative
    #[clap(long)]
    pub(crate) absolute: bool,

    /// show long version with extra metadata
    #[clap(long)]
    pub(crate) long: bool,

    /// show files only, no totals
    #[clap(long)]
    pub(crate) files_only: bool,
}

/// Arguments for the `schema` subcommand
#[derive(Parser)]
pub(crate) struct SchemaArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

/// Arguments for the `stats` subcommand
#[derive(Parser)]
pub(crate) struct StatsArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

//
// // edit commands
//

/// Arguments for the `drop` subcommand
#[derive(Parser)]
pub(crate) struct DropArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

/// Arguments for the `cast` subcommand
#[derive(Parser)]
pub(crate) struct CastArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

/// Arguments for the `merge` subcommand
#[derive(Parser)]
pub(crate) struct MergeArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

/// Arguments for the `partition` subcommand
#[derive(Parser)]
pub(crate) struct PartitionArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}

/// Arguments for the `pl` subcommand
#[derive(Parser)]
pub(crate) struct PlArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,
}
