use super::subcommands::*;
use crate::TablCliError;
use clap::{Parser, Subcommand};
use color_print::cstr;
use std::path::PathBuf;

pub(crate) async fn run_cli() -> Result<(), TablCliError> {
    let args = Cli::parse();

    if args.version {
        println!("{}", env!("GIT_DESCRIPTION"));
        std::process::exit(0);
    }

    match args.command {
        Some(Subcommands::Ls(args)) => ls_command(args).await,
        Some(Subcommands::Schema(args)) => schema_command(args).await,
        Some(Subcommands::Schemas(args)) => schemas_command(args).await,
        _ => data_command(args.data_args).await,
    }
}

/// Utility for creating and managing MESC RPC configurations
#[derive(Clone, Parser)]
#[clap(
    author,
    about = cstr!("<white><bold>tbl</bold></white> is a tool for reading and editing tabular data files"),
    override_usage = cstr!("<white><bold>tbl</bold></white> has two modes
1. Summary mode: <white><bold>tbl [ls | schema | schemas] [SUMMARY_OPTIONS]</bold></white>
2. Data mode:    <white><bold>tbl [DATA_OPTIONS]</bold></white>

Get help with <white><bold>SUMMARY_OPTIONS</bold></white> using <white><bold>tbl [ls | schema | schemas] -h</bold></white>

Data mode is the default mode. <white><bold>DATA_OPTIONS</bold></white> are documented below
"),
    long_about = None,
    disable_help_subcommand = true,
    disable_help_flag = true,
    disable_version_flag = true,
    args_conflicts_with_subcommands = true,
    subcommand_help_heading = "Optional Subcommands",
    styles=crate::styles::get_styles()
)]
pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) command: Option<Subcommands>,

    ///                    display help message
    #[clap(short, long, verbatim_doc_comment, action = clap::ArgAction::HelpLong, help_heading = "General Options")]
    help: Option<bool>,

    ///                    display version
    #[clap(
        short = 'V',
        long,
        verbatim_doc_comment,
        help_heading = "General Options"
    )]
    version: bool,

    #[clap(flatten)]
    data_args: DataArgs,
}

/// Define your subcommands as an enum
#[derive(Clone, Subcommand)]
#[command()]
pub(crate) enum Subcommands {
    /// Display list of tabular files
    Ls(LsArgs),

    /// Display each schema present among selected files
    Schema(SchemaArgs),

    /// Display single summary of all schemas
    Schemas(SchemasArgs),

    /// Load, transform, and output file data [default subcommand]
    #[command(hide = true)]
    Data,
}

/// Arguments for the `schema` subcommand
#[derive(Clone, Parser)]
pub(crate) struct LsArgs {
    /// display help message
    #[clap(short, long, action = clap::ArgAction::HelpLong, help_heading = "General Options")]
    help: Option<bool>,

    /// input path(s) to use
    #[clap()]
    pub(crate) paths: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(short, long)]
    pub(crate) tree: bool,

    /// show absolute paths instead of relative
    #[clap(long)]
    pub(crate) absolute: bool,

    /// display bytes stats
    #[clap(long)]
    pub(crate) bytes: bool,

    /// display stats of each schema group
    #[clap(long)]
    pub(crate) stats: bool,

    /// number of file names to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// sort by number of rows, files, or bytes
    #[clap(long, default_value = "bytes")]
    pub(crate) sort: String,
}

/// Arguments for the `schema` subcommand
#[derive(Clone, Parser)]
pub(crate) struct SchemaArgs {
    /// display help message
    #[clap(short, long, action = clap::ArgAction::HelpLong, help_heading = "General Options")]
    help: Option<bool>,

    /// input path(s) to use
    #[clap()]
    pub(crate) paths: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(short, long)]
    pub(crate) tree: bool,

    /// display bytes stats
    #[clap(long)]
    pub(crate) bytes: bool,

    /// display stats of each schema group
    #[clap(long)]
    pub(crate) stats: bool,

    /// columns to print
    #[clap(long)]
    pub(crate) columns: Option<Vec<String>>,

    /// number of schemas to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// show examples
    #[clap(long)]
    pub(crate) examples: bool,

    /// show absolute paths in examples
    #[clap(long)]
    pub(crate) absolute: bool,

    /// sort by number of rows, files, or bytes
    #[clap(long, default_value = "bytes")]
    pub(crate) sort: String,
}

/// Arguments for the `schema` subcommand
#[derive(Clone, Parser)]
pub(crate) struct SchemasArgs {
    /// display help message
    #[clap(short, long, action = clap::ArgAction::HelpLong, help_heading = "General Options")]
    help: Option<bool>,

    /// input path(s) to use
    #[clap()]
    pub(crate) paths: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(short, long)]
    pub(crate) tree: bool,

    /// sort by number of rows, files, or bytes
    #[clap(long, default_value = "bytes")]
    pub(crate) sort: String,
}

/// Arguments for the `data` subcommand
#[derive(Clone, Parser)]
pub(crate) struct DataArgs {
    //
    // // input options
    //
    ///                       input path(s) to use
    #[clap(
        verbatim_doc_comment,
        help_heading = "Input Options",
        display_order = 1
    )]
    pub(crate) paths: Option<Vec<PathBuf>>,

    ///                   recursively use all files in tree as inputs
    #[clap(short, long, verbatim_doc_comment, help_heading = "Input Options")]
    pub(crate) tree: bool,

    //
    // // transform options
    //
    /// add new columns
    #[clap(long, help_heading = "Transform Options", value_name="NEW_COLS", num_args(1..))]
    pub(crate) with_columns: Vec<String>,

    /// select only these columns
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) select: Vec<String>,

    /// drop column(s)
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) drop: Vec<String>,

    /// rename column(s), syntax OLD_NAME=NEW_NAME
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) rename: Vec<String>,

    /// change column type(s), syntax COLUMN=TYPE
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) cast: Vec<String>,

    /// filter rows by values, syntax COLUMN=VALUE
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) filter: Vec<String>,

    /// sort rows, sytax COLUMN[:desc]
    #[clap(long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) sort: Vec<String>,

    /// keep only the first n rows [alias = `limit`]
    #[clap(long, aliases = ["limit"], help_heading = "Transform Options")]
    pub(crate) head: Option<usize>,

    /// keep only the last n rows
    #[clap(long, help_heading = "Transform Options")]
    pub(crate) tail: Option<usize>,

    /// skip the first n rows of table
    #[clap(long, help_heading = "Transform Options")]
    pub(crate) offset: Option<usize>,

    /// compute value counts of column(s)
    #[clap(long, help_heading = "Transform Options", value_name = "COLUMN")]
    pub(crate) value_counts: Option<String>,

    //
    // // output options
    //
    /// output data as csv
    #[clap(long, help_heading = "Output Options")]
    pub(crate) csv: bool,

    /// output data as json
    #[clap(long, help_heading = "Output Options")]
    pub(crate) json: bool,

    /// modify files in place
    #[clap(long, help_heading = "Output Options")]
    pub(crate) inplace: bool,

    /// write all data to a single new file
    #[clap(long, help_heading = "Output Options", value_name = "FILE_PATH")]
    pub(crate) output_file: Option<PathBuf>,

    /// rewrite all files into this output directory
    #[clap(long, help_heading = "Output Options", value_name = "DIR_PATH")]
    pub(crate) output_dir: Option<PathBuf>,

    /// prefix to add to output filenames
    #[clap(long, help_heading = "Output Options", value_name = "PREFIX")]
    pub(crate) output_prefix: Option<String>,

    /// postfix to add to output filenames
    #[clap(long, help_heading = "Output Options", value_name = "POSTFIX")]
    pub(crate) output_postfix: Option<String>,

    /// partition output over this column
    #[clap(long, help_heading = "Output Options", value_name = "COLUMN")]
    pub(crate) partition: Option<String>,

    /// partition mode, by range of values in each partition
    #[clap(long, help_heading = "Output Options", value_name = "SIZE")]
    pub(crate) partition_by_value: Option<String>,

    /// parition mode, by max bytes per partition
    #[clap(long, help_heading = "Output Options", value_name = "BYTES")]
    pub(crate) partition_by_bytes: Option<String>,

    /// parition mode, by max rows per partition
    #[clap(long, help_heading = "Output Options", value_name = "BYTES")]
    pub(crate) partition_by_rows: Option<String>,

    /// load as DataFrame in interactive python session
    #[clap(long, help_heading = "Output Options")]
    pub(crate) df: bool,

    /// load as LazyFrame in interactive python session
    #[clap(long, help_heading = "Output Options")]
    pub(crate) lf: bool,

    /// python executable to use with --df or --lf
    #[clap(long, help_heading = "Output Options")]
    pub(crate) executable: Option<String>,

    /// confirm that files should be edited
    #[clap(long, help_heading = "Output Options")]
    pub(crate) confirm: bool,

    /// dry run without editing files
    #[clap(long, help_heading = "Output Options")]
    pub(crate) dry: bool,
}
