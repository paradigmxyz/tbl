use super::subcommands::*;
use crate::TblCliError;
use clap::{Parser, Subcommand};
use color_print::cstr;
use std::path::PathBuf;

pub(crate) async fn run_cli() -> Result<(), TblCliError> {
    match Cli::parse().command {
        // read
        Commands::Ls(args) => ls_command(args).await,
        Commands::Schema(args) => schema_command(args).await,
        Commands::Cat(args) => cat_command(args).await,
        Commands::Head(args) => head_command(args).await,
        Commands::Tail(args) => tail_command(args).await,
        Commands::Count(args) => count_command(args).await,
        // migrate
        Commands::Migrate(args) => migrate_command(args).await,
        // partition
        Commands::Merge(args) => merge_command(args).await,
        Commands::Partition(args) => partition_command(args),
        // interactive
        Commands::Df(args) => df_command(args),
        Commands::Lf(args) => lf_command(args),
    }
}

/// Utility for creating and managing MESC RPC configurations
#[derive(Parser)]
#[clap(author, version, about, long_about = None, disable_help_subcommand = true, styles=get_styles(),
    override_help = cstr!(
        r#"
<rgb(0,225,0)><bold>Usage:</bold></rgb(0,225,0)> <white><bold>tbl</bold></white> COMMAND [OPTIONS]

<rgb(0,225,0)><bold>Display Commands</bold></rgb(0,225,0)>
    <white><bold>ls</bold></white>                      list files
    <white><bold>schema</bold></white>                  display schemas of files
    <white><bold>cat</bold></white>                     display contents of files
    <white><bold>head</bold></white>                    display first N rows of files
    <white><bold>tail</bold></white>                    display last N rows of files
    <white><bold>count</bold></white>                   display count statistics for files

<rgb(0,225,0)><bold>Migration Commands</bold></rgb(0,225,0)>
    <white><bold>migrate add</bold></white>             add column to files
    <white><bold>migrate drop</bold></white>            drop column from files
    <white><bold>migrate rename</bold></white>          rename column
    <white><bold>migrate cast</bold></white>            change type of column

<rgb(0,225,0)><bold>Partition Commands</bold></rgb(0,225,0)>
    <white><bold>merge</bold></white>                   merge files into one file
    <white><bold>partition</bold></white>               repartition files

<rgb(0,225,0)><bold>Interactive Commands</bold></rgb(0,225,0)>
    <white><bold>df</bold></white>                      load files as DataFrame in IPython session
    <white><bold>lf</bold></white>                      load files as LazyFrame in IPython session

<rgb(0,225,0)><bold>Specifying Input Files</bold></rgb(0,225,0)>
    <white><bold>[PATH1 [PATH2 [...]]]</bold></white>   list of input paths, files or directories
    <white><bold>--tree</bold></white>                  recursive scan all files in directories
    <white><bold>[]</bold></white>                      default input is all files in current directory
<rgb(100,100,100)>(every command takes these arguments to specify input files)</rgb(100,100,100)>
"#
    )
    )
]

pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) command: Commands,
}

pub(crate) fn get_styles() -> clap::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new().bold().fg_color(Some(green));
    let arg = anstyle::Style::new().bold().fg_color(Some(white));
    let comment = anstyle::Style::new().fg_color(Some(grey));
    clap::builder::Styles::styled()
        .header(title)
        .error(comment)
        .usage(title)
        .literal(arg)
        .placeholder(comment)
        .valid(title)
        .invalid(comment)
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
    /// Show first N rows of a dataset
    Cat(CatArgs),
    /// Show first N rows of a dataset (alias for `cat`)
    Head(HeadArgs),
    /// Show last N rows of a dataset (alias for `cat --tail`)
    Tail(TailArgs),
    /// Count value occurences within column(s) of data
    Count(CountArgs),
    //
    // // edit commands
    //
    /// Migrate commands
    #[command(subcommand)]
    Migrate(MigrateArgs),
    //
    // // migrate commands
    //
    /// Merge tabular files
    Merge(MergeArgs),
    /// Partition tabular files
    Partition(PartitionArgs),
    //
    // // interactive commands
    //
    /// Load inputs as a dataframe in an interactive python session
    Df(DfArgs),
    /// Load inputs as a lazyframe in an interactive python session
    Lf(LfArgs),
}

//
// // read commands
//

#[derive(Parser)]
pub(crate) struct InputArgs {
    /// input path(s) to use
    #[clap(short, long)]
    pub(crate) paths: Option<Vec<PathBuf>>,

    /// recursively list all files in tree
    #[clap(short, long)]
    pub(crate) tree: bool,
}

/// Arguments for the `ls` subcommand
#[derive(Parser)]
pub(crate) struct LsArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// number of file names to print
    #[clap(short, long)]
    pub(crate) n: Option<usize>,

    /// skip summary stats
    #[clap(long)]
    pub(crate) no_stats: bool,

    /// show absolute paths instead of relative
    #[clap(short, long)]
    pub(crate) absolute: bool,

    /// display schemas instead of paths
    #[clap(short, long)]
    pub(crate) schema: bool,

    /// display bytes stats
    #[clap(short, long)]
    pub(crate) bytes: bool,

    /// display stats of each schema group
    #[clap(long)]
    pub(crate) schema_stats: bool,
}

/// Arguments for the `schema` subcommand
#[derive(Parser)]
pub(crate) struct SchemaArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

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

#[derive(Parser)]
pub(crate) struct CatArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// columns to print
    #[clap(long)]
    pub(crate) columns: Option<Vec<String>>,

    /// number of file names to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// sort before showing preview
    #[clap(short, long)]
    pub(crate) sort: Option<Vec<String>>,

    /// limit to this number of rows
    #[clap(short, long)]
    pub(crate) limit: Option<usize>,

    /// offset before printing head
    #[clap(short, long)]
    pub(crate) offset: Option<usize>,

    /// print tail instead of head
    #[clap(long)]
    pub(crate) tail: bool,
}

#[derive(Parser)]
pub(crate) struct HeadArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// columns to print
    #[clap(long)]
    pub(crate) columns: Option<Vec<String>>,

    /// number of file names to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// sort before showing preview
    #[clap(short, long)]
    pub(crate) sort: Option<Vec<String>>,

    /// offset before printing head
    #[clap(short, long)]
    pub(crate) offset: Option<usize>,
}

#[derive(Parser)]
pub(crate) struct TailArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// columns to print
    #[clap(long)]
    pub(crate) columns: Option<Vec<String>>,

    /// number of file names to print
    #[clap(long)]
    pub(crate) n: Option<usize>,

    /// sort before showing preview
    #[clap(short, long)]
    pub(crate) sort: Option<Vec<String>>,

    /// limit to this number of rows
    #[clap(short, long)]
    pub(crate) limit: Option<usize>,
}

#[derive(Parser)]
pub(crate) struct CountArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// columns to print values of
    #[clap(short, long)]
    pub(crate) columns: Vec<String>,

    /// show multi-column value columns
    #[clap(short, long)]
    pub(crate) group: Vec<String>,

    /// number of values to display
    #[clap(long)]
    pub(crate) n: Option<usize>,
}

//
// // migrate commands
//

#[derive(Subcommand)]
#[command()]
pub(crate) enum MigrateArgs {
    /// Insert columns into tabular files
    Insert(InsertArgs),
    /// Drop columns from tabular files
    Drop(DropArgs),
    /// Cast columns of tabular files
    Cast(CastArgs),
}

#[derive(Parser)]
pub(crate) struct InsertArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// column specifications, in pairs of COLUMN_NAME DTYPE
    pub(crate) new_columns: Vec<String>,

    /// output directory to write modified files
    #[clap(long)]
    pub(crate) output_dir: Option<PathBuf>,

    /// index of inserted column(s)
    #[clap(long)]
    pub(crate) index: Option<Vec<usize>>,

    /// default value(s) of inserted column(s)
    #[clap(long)]
    pub(crate) default: Option<Vec<String>>,

    /// confirm that files should be edited
    #[clap(long)]
    pub(crate) confirm: bool,

    /// prefix to add to output filenames
    #[clap(long)]
    pub(crate) output_prefix: Option<String>,

    /// postfix to add to output filenames
    #[clap(long)]
    pub(crate) output_postfix: Option<String>,
}

/// Arguments for the `drop` subcommand
#[derive(Parser)]
pub(crate) struct DropArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// columns to drop
    #[clap()]
    pub(crate) columns: Vec<String>,

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
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,
}

//
// // partition commands
//

/// Arguments for the `merge` subcommand
#[derive(Parser)]
pub(crate) struct MergeArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// output path to use
    #[clap()]
    pub(crate) output_path: PathBuf,

    /// keep original files after merging
    #[clap(long)]
    pub(crate) keep: bool,

    /// confirm merge
    #[clap(long)]
    pub(crate) confirm: bool,
}

/// Arguments for the `partition` subcommand
#[derive(Parser)]
pub(crate) struct PartitionArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// keep original files after merging
    #[clap(long)]
    pub(crate) keep: bool,

    /// confirm merge
    #[clap(long)]
    pub(crate) confirm: bool,
}

//
// // interactive commands
//

/// Arguments for the `df` subcommand
#[derive(Parser)]
pub(crate) struct DfArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// python executable to use
    #[clap(short, long)]
    pub(crate) executable: Option<String>,

    /// load lazily as LazyFrame instead of DataFrame
    #[clap(short, long)]
    pub(crate) lazy: bool,
}

/// Arguments for the `lf` subcommand
#[derive(Parser)]
pub(crate) struct LfArgs {
    /// input path arguments
    #[clap(flatten)]
    pub(crate) inputs: InputArgs,

    /// python executable to use
    #[clap(short, long)]
    pub(crate) executable: Option<String>,
}
