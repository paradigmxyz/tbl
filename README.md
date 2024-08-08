
# tbl ┳━┳

`tbl` is a tool for reading and editing tabular data files like parquet

Goals of `tbl`:
- make it effortless to read, edit, and manage parquet datasets
- use a cli-native version of polars syntax, so if you know python polars you already know `tbl`

## Contents
1. [Installation](#installation)
2. [Example Usage](#example-usage)
3. [API Reference](#api-reference)
    1. [Listing files](#listing-files)
    2. [Looking up schemas](#looking-up-schemas)
    3. [Selecting input files](#selecting-input-files)
    4. [Performing edits](#performing-edits)
    5. [Selecting output mode](#selecting-output-mode)
4. [FAQ](#faq)

## Installation

`cargo install tbl`

## Example Usage

### Listing files

`tbl` can list files and display their statistics, similar to the `ls` cli command.

The command `tbl ls` produces output:

```
blocks__00000000_to_00000999.parquet
blocks__00001000_to_00001999.parquet
blocks__00002000_to_00002999.parquet
blocks__00003000_to_00003999.parquet
blocks__00004000_to_00004999.parquet
blocks__00005000_to_00005999.parquet
blocks__00006000_to_00006999.parquet
blocks__00007000_to_00007999.parquet
blocks__00008000_to_00008999.parquet
blocks__00009000_to_00009999.parquet
... 19,660 files not shown
19,041,325 rows stored in 1.05 GB across 19,708 tabular files
```

### Looking up schemas

`tbl` can display the schemas of parquet files.

The command `tbl schema` produces output:

```
1 unique schema, 19,041,325 rows, 19,708 files, 1.05 GB

     column name  │   dtype  │  disk size  │  full size  │  disk %
──────────────────┼──────────┼─────────────┼─────────────┼────────
      block_hash  │  binary  │  649.97 MB  │  657.93 MB  │  63.78%
          author  │  binary  │   40.52 MB  │   40.59 MB  │   3.98%
    block_number  │     u32  │   76.06 MB  │   75.75 MB  │   7.46%
        gas_used  │     u64  │   84.23 MB  │  133.29 MB  │   8.26%
      extra_data  │  binary  │   46.66 MB  │   76.91 MB  │   4.58%
       timestamp  │     u32  │   76.06 MB  │   75.75 MB  │   7.46%
base_fee_per_gas  │     u64  │   41.85 MB  │   49.58 MB  │   4.11%
        chain_id  │     u64  │    3.74 MB  │    3.70 MB  │   0.37%
```

### Selecting input files

`tbl` can operate on one file, or many files across multiple directories.

These input selection options can be used with each `tbl` subcommand:

|  | option |
| --- | --- |
| select all tabular files in current directory | (default behavior) |
| select a single file | `tbl /path/to/file.parquet` |
| select files using a glob | `tbl *.parquet` |
| select files from multiple directories | `tbl /path/to/dir1 /path/to/dir2` |
| select files recursively | `tbl /path/to/dir --tree` |

### Performing edits

`tbl` can perform many different operations on the selected files:

| operation | command |
| --- | --- |
| Rename a column | `tbl --rename old_name=new_name` |
| Cast to a new type | `tbl --cast col1=u64 col2=String` |
| Add new columns | `tbl --with-columns name:String date:Date=2024-01-01` |
| Drop columns | `tbl --drop col1 col2 col3` |
| Filter rows | `tbl --filter col1=val1` |
| Sort rows | `tbl --sort col1 col2:desc` |
| Select columns | `tbl --select col1 col2 col3` |

### Selecting output mode

`tbl` can output its results in many different forms:

| output mode | `tbl` option |
| --- | --- |
| output all results to single file | `--output-file /path/to/file.parquet` |
| modify each file in place | `--inplace` |
| create equivalent files in a new dir | `--output-dir /path/to/dir` |
| load dataframe in interactive python session | `--df` |
| output data to stdout | (default behavior) |

## API Reference

##### Output of `tbl -h`:

```markdown
tbl is a tool for reading and editing tabular data files

Usage: tbl has two modes
1. Summary mode: tbl [ls | schema | schemas] [SUMMARY_OPTIONS]
2. Data mode:    tbl [DATA_OPTIONS]

Get help with SUMMARY_OPTIONS using tbl [ls | schema | schemas] -h

Data mode is the default mode. DATA_OPTIONS are documented below

Optional Subcommands:
  ls       Display list of tabular files
  schema   Display each schema present among selected files
  schemas  Display single summary of all schemas

General Options:
  -h, --help                        display help message
  -V, --version                     display version

Input Options:
  [PATHS]...                        input path(s) to use
  -t, --tree                        recursively use all files in tree as inputs

Transform Options:
      --with-columns <NEW_COLS>...  add new columns, syntax NAME:TYPE [alias = `--with`]
      --select <SELECT>...          select only these columns
      --drop <DROP>...              drop column(s)
      --rename <RENAME>...          rename column(s), syntax OLD_NAME=NEW_NAME
      --cast <CAST>...              change column type(s), syntax COLUMN=TYPE
      --filter <FILTER>...          filter rows by values, syntax COLUMN=VALUE
      --sort <SORT>...              sort rows, sytax COLUMN[:desc]
      --head <HEAD>                 keep only the first n rows [alias = `--limit`]
      --tail <TAIL>                 keep only the last n rows
      --offset <OFFSET>             skip the first n rows of table
      --value-counts <COLUMN>       compute value counts of column(s)

Output Options:
      --no-summary                  skip printing a summary
      --csv                         output data as csv
      --json                        output data as json
      --inplace                     modify files in place
      --output-file <FILE_PATH>     write all data to a single new file
      --output-dir <DIR_PATH>       rewrite all files into this output directory
      --output-prefix <PREFIX>      prefix to add to output filenames
      --output-postfix <POSTFIX>    postfix to add to output filenames
      --partition <COLUMN>          partition output over this column
      --partition-by-value <SIZE>   partition mode, by range of values per partition
      --partition-by-bytes <BYTES>  partition mode, by max bytes per partition
      --partition-by-rows <ROWS>    partition mode, by max rows per partition
      --df                          load as DataFrame in interactive python session
      --lf                          load as LazyFrame in interactive python session
      --executable <EXECUTABLE>     python executable to use with --df or --lf
      --confirm                     confirm that files should be edited
      --dry                         dry run without editing files
```

##### Output of `tbl ls -h`:

```markdown
Display list of tabular files, similar to the cli `ls` command

Usage: tbl ls [OPTIONS] [PATHS]...

Arguments:
  [PATHS]...  input path(s) to use

Options:
  -t, --tree         recursively list all files in tree
      --absolute     show absolute paths instead of relative
      --bytes        display bytes stats
      --stats        display stats of each schema group
      --n <N>        number of file names to print
      --sort <SORT>  sort by number of rows, files, or bytes [default: bytes]

General Options:
  -h, --help  display help message
```

##### Output of `tbl schema -h`:

```markdown
Display table representation of each schema in the selected files

Usage: tbl schema [OPTIONS] [PATHS]...

Arguments:
  [PATHS]...  input path(s) to use

Options:
  -t, --tree               recursively list all files in tree
      --bytes              display bytes stats
      --stats              display stats of each schema group
      --columns <COLUMNS>  columns to print
      --n <N>              number of schemas to print
      --examples           show examples
      --absolute           show absolute paths in examples
      --sort <SORT>        sort by number of rows, files, or bytes [default: bytes]

General Options:
  -h, --help  display help message
```

##### Output of `tbl schemas -h`:

```markdown
Display table representation of each schema in the selected files

Usage: tbl schema [OPTIONS] [PATHS]...

Arguments:
  [PATHS]...  input path(s) to use

Options:
  -t, --tree               recursively list all files in tree
      --bytes              display bytes stats
      --stats              display stats of each schema group
      --columns <COLUMNS>  columns to print
      --n <N>              number of schemas to print
      --examples           show examples
      --absolute           show absolute paths in examples
      --sort <SORT>        sort by number of rows, files, or bytes [default: bytes]

General Options:
  -h, --help  display help message
```

## FAQ

What other tools exist for interacting with parquet from the command line?
- [duckdb](https://duckdb.org/docs/api/cli/overview)
- [pqrs](https://github.com/manojkarthick/pqrs)
- [parquet-cli](https://github.com/apache/parquet-java/blob/master/parquet-cli/README.md)
