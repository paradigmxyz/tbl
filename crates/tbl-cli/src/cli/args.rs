use super::subcommands::*;
use crate::TblCliError;
use clap::{Parser, Subcommand};
use color_print::cstr;
use std::path::PathBuf;

pub(crate) async fn run_cli() -> Result<(), TblCliError> {
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
1. Summary mode: <white><bold>tbl [ls | schema] [SUMMARY_OPTIONS]</bold></white>
2. Data mode:    <white><bold>tbl [DATA_OPTIONS]</bold></white>

Get help with <white><bold>SUMMARY_OPTIONS</bold></white> using <white><bold>tbl [ls | schema] -h</bold></white>

Data mode is the default mode. <white><bold>DATA_OPTIONS</bold></white> are documented below
"),
    after_help = cstr!("<rgb(0,225,0)><bold>Output Modes:</bold></rgb(0,225,0)>
<white><bold>1.</bold></white> output results in <white><bold>single file</bold></white>   <white><bold>--output-file</bold></white> /path/to/file.parquet
<white><bold>2.</bold></white> modify each file <white><bold>inplace</bold></white>        <white><bold>--inplace</bold></white>
<white><bold>3.</bold></white> copy files into a <white><bold>new dir</bold></white>       <white><bold>--output-dir</bold></white> /path/to/dir
<white><bold>4.</bold></white> load as <white><bold>interactive</bold></white> python      <white><bold>--df | --lf</bold></white>
<white><bold>5.</bold></white> output data to <white><bold>stdout</bold></white>           (default behavior)"),
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

    ///                   display help message
    #[clap(short, long, verbatim_doc_comment, action = clap::ArgAction::HelpLong, help_heading = "General Options")]
    help: Option<bool>,

    ///                   display version
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
    /// Display list of tabular files, similar to the cli `ls` command
    Ls(LsArgs),

    /// Display table representation of each schema in the selected files
    Schema(SchemaArgs),

    /// Display single summary of all schemas
    #[command(hide = true)]
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
    ///                      input path(s) to use
    #[clap(
        verbatim_doc_comment,
        help_heading = "Input Options",
        display_order = 1
    )]
    pub(crate) paths: Option<Vec<PathBuf>>,

    ///                  recursively use all files in tree as inputs
    #[clap(short, long, verbatim_doc_comment, help_heading = "Input Options")]
    pub(crate) tree: bool,

    //
    // // transform options
    //
    /// select only these columns [alias --columns]
    #[clap(
        short,
        long,
        help = cstr!("select only these columns [alias <white><bold>--select</bold></white>]"),
        help_heading = "Transform Options",
        aliases = ["select"],
        num_args(1..)
    )]
    pub(crate) columns: Option<Vec<String>>,

    /// drop column(s)
    #[clap(short, long, help_heading = "Transform Options", num_args(1..))]
    pub(crate) drop: Option<Vec<String>>,

    /// add new columns, syntax NAME:TYPE [alias --with]
    #[clap(
        long,
        help = cstr!("insert columns, syntax <white><bold>NAME:TYPE</bold></white> [alias <white><bold>--with</bold></white>]"),
        help_heading = "Transform Options",
        value_name="NEW_COL",
        num_args(1..),
        aliases = ["with"]
    )]
    pub(crate) with_columns: Option<Vec<String>>,

    /// rename column(s), syntax OLD_NAME=NEW_NAME
    #[clap(
        short,
        long,
        help = cstr!("rename column(s), syntax <white><bold>OLD_NAME=NEW_NAME</bold></white>"),
        help_heading = "Transform Options",
        num_args(1..)
    )]
    pub(crate) rename: Option<Vec<String>>,

    /// change column type(s), syntax COLUMN=TYPE
    #[clap(
        long,
        help = cstr!("change column type(s), syntax <white><bold>COLUMN=TYPE</bold></white>"),
        help_heading = "Transform Options",
        num_args(1..)
    )]
    pub(crate) cast: Option<Vec<String>>,

    /// filter rows by values, syntax COLUMN=VALUE
    #[clap(
        short,
        long,
        help = cstr!("filter rows by values, syntax <white><bold>COLUMN=VALUE</bold></white>
    or <white><bold>COLUMN.is_null</bold></white> or <white><bold>COLUMN.is_not_null</bold></white>"),
        help_heading = "Transform Options",
        num_args(1..)
    )]
    pub(crate) filter: Option<Vec<String>>,

    /// sort rows, syntax COLUMN[:desc]
    #[clap(
        short,
        long,
        help = cstr!("sort rows, syntax <white><bold>COLUMN[:desc]</bold></white>"),
        help_heading = "Transform Options",
        num_args(1..)
    )]
    pub(crate) sort: Option<Vec<String>>,

    /// keep only the first n rows [alias --limit]
    #[clap(
        long,
        help = cstr!("keep only the first n rows [alias <white><bold>--limit</bold></white>]"),
        help_heading = "Transform Options",
        aliases = ["limit"]
    )]
    pub(crate) head: Option<usize>,

    /// keep only the last n rows
    #[clap(long, help_heading = "Transform Options")]
    pub(crate) tail: Option<usize>,

    /// skip the first n rows of table
    #[clap(long, help_heading = "Transform Options")]
    pub(crate) offset: Option<usize>,

    /// compute value counts of column(s)
    /// count valu
    #[clap(long, help_heading = "Transform Options", value_name = "COLUMN")]
    pub(crate) value_counts: Option<String>,

    //
    // // output options
    //
    /// skip printing a summary
    #[clap(long, help_heading = "Output Options")]
    pub(crate) no_summary: bool,

    /// number of rows to print in stdout, all for all
    #[clap(
        short,
        long,
        help = cstr!("number of rows to print in stdout, <white><bold>all</bold></white> for all"),
        help_heading = "Output Options"
    )]
    pub(crate) n: Option<String>,

    /// output data as csv
    #[clap(long, help_heading = "Output Options")]
    pub(crate) csv: bool,

    /// output data as json
    #[clap(long, help_heading = "Output Options")]
    pub(crate) json: bool,

    /// output data as json lines
    #[clap(long, help_heading = "Output Options")]
    pub(crate) jsonl: bool,

    /// encode binary columns as hex for output
    #[clap(long, help_heading = "Output Options")]
    pub(crate) hex: bool,

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
    #[clap(long, help_heading = "Output Options", value_name = "PRE-FIX")]
    pub(crate) output_prefix: Option<String>,

    /// postfix to add to output filenames
    #[clap(long, help_heading = "Output Options", value_name = "POST-FIX")]
    pub(crate) output_postfix: Option<String>,

    /// partition output over this column
    #[clap(
        long,
        help_heading = "Output Options",
        value_name = "COLUMN",
        hide = true
    )]
    pub(crate) partition: Option<String>,

    /// partition mode, by range of values per partition
    #[clap(
        long,
        help_heading = "Output Options",
        value_name = "SIZE",
        hide = true
    )]
    pub(crate) partition_by_value: Option<String>,

    /// partition mode, by max bytes per partition
    #[clap(
        long,
        help_heading = "Output Options",
        value_name = "BYTES",
        hide = true
    )]
    pub(crate) partition_by_bytes: Option<String>,

    /// partition mode, by max rows per partition
    #[clap(
        long,
        help_heading = "Output Options",
        value_name = "ROWS",
        hide = true
    )]
    pub(crate) partition_by_rows: Option<String>,

    /// load as DataFrame in interactive python session
    #[clap(long, help_heading = "Output Options")]
    pub(crate) df: bool,

    /// load as LazyFrame in interactive python session
    #[clap(long, help_heading = "Output Options")]
    pub(crate) lf: bool,

    /// python executable to use with --df or --lf
    #[clap(
        long,
        help = cstr!("python executable to use with <white><bold>--df</bold></white> or <white><bold>--lf</bold></white>"),
        help_heading = "Output Options"
    )]
    pub(crate) executable: Option<String>,

    /// confirm that files should be edited
    #[clap(long, help_heading = "Output Options")]
    pub(crate) confirm: bool,

    /// dry run without editing files
    #[clap(long, help_heading = "Output Options")]
    pub(crate) dry: bool,
}
