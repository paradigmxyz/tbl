use super::subcommands::*;
use crate::TablCliError;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub(crate) async fn run_cli() -> Result<(), TablCliError> {
    match Cli::parse().command {
        Commands::Ls(args) => ls_command(args).await,
        Commands::Schema(args) => schema_command(args).await,
        Commands::Cast(args) => cast_command(args),
        Commands::Drop(args) => drop_command(args).await,
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
    #[clap()]
    pub(crate) inputs: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(long)]
    pub(crate) tree: bool,

    /// show absolute paths instead of relative
    #[clap(long)]
    pub(crate) absolute: bool,

    /// print top n schemas
    #[clap(long)]
    pub(crate) n_schemas: Option<usize>,

    /// print example paths of each schema
    #[clap(long)]
    pub(crate) include_example_paths: bool,

    /// sort schemas by row count, file count, or byte count
    #[clap(long, default_value = "bytes")]
    pub(crate) sort: String,
}

/// Arguments for the `stats` subcommand
#[derive(Parser)]
pub(crate) struct StatsArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(long)]
    pub(crate) tree: bool,
}

//
// // edit commands
//

/// Arguments for the `drop` subcommand
#[derive(Parser)]
pub(crate) struct DropArgs {
    /// columns to drop
    #[clap()]
    pub(crate) columns: Vec<String>,

    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) inputs: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(long)]
    pub(crate) tree: bool,

    /// confirm that files should be edited
    #[clap(long)]
    pub(crate) confirm: bool,

    /// prefix to add to output filenames
    #[clap(long)]
    pub(crate) output_prefix: Option<String>,

    /// postfix to add to output filenames
    #[clap(long)]
    pub(crate) output_postfix: Option<String>,

    /// output directory to write modified files
    #[clap(long)]
    pub(crate) output_dir: Option<PathBuf>,

    /// show output paths
    #[clap(long)]
    pub(crate) show_output_paths: bool,
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
