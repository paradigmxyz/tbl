
# tbl ┳━┳

`tbl` is a cli tool for reading and editing parquet files

#### Goals of `tbl`:
- be a swiss army knife for reading/editing parquet (like [`jq`](https://github.com/jqlang/jq) is for JSON)
- make it effortless to manage large multi-schema parquet datasets
- use a cli-native version of [polars](https://github.com/pola-rs/polars) syntax, so if you know python polars you already know `tbl`

#### Example use cases:
- quickly look up schemas, row counts, and per-column storage usage
- migrate from one schema to another, like add/remove/rename a column
- perform these operations on multiple files in parallel

## Contents
1. [Installation](#installation)
2. [Example Usage](#example-usage)
    1. [Listing files](#listing-files)
    2. [Looking up schemas](#looking-up-schemas)
    3. [Selecting input files](#selecting-input-files)
    4. [Performing edits](#performing-edits)
    5. [Selecting output mode](#selecting-output-mode)
4. [API Reference](#api-reference)
    1. [`tbl`](#tbl)
    2. [`tbl ls`](#tbl-ls)
    3. [`tbl schema`](#tbl-schema)
6. [FAQ](#faq)
    1. [What is parquet?](#what-is-parquet)
    2. [What other parquet cli tools exist?](#what-other-parquet-cli-tools-exist)
    3. [Why use `tbl` when `duckdb` has a cli?](#why-use-tbl-when-duckdb-has-a-cli)
    4. [What is the plan for `tbl`?](#what-is-the-plan-for-tbl)

## Installation

##### Install from crates.io
```bash
cargo install tbl-cli
```

##### Install from source
```bash
git clone https://github.com/paradigmxyz/tbl
cd tbl
cargo install --path crates/tbl-cli
```

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

See full list of `tbl ls` options [below](#tbl-ls).

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

See full list of `tbl schema` options [below](#tbl-schema).

### Selecting input files

`tbl` can operate on one file, or many files across multiple directories.

These input selection options can be used with each `tbl` subcommand:

| input selection | command |
| --- | --- |
| Select all tabular files in current directory | `tbl` (default behavior) |
| Select a single file | `tbl /path/to/file.parquet` |
| Select files using a glob | `tbl *.parquet` |
| Select files from multiple directories | `tbl /path/to/dir1 /path/to/dir2` |
| Select files recursively | `tbl /path/to/dir --tree` |

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

See full list of transformation operations [below](#tbl).

### Selecting output mode

`tbl` can output its results in many different modes:

| output mode | description | command |
| --- | --- | --- |
| Single File | output all results to single file | `tbl --output-file /path/to/file.parquet` |
| Inplace | modify each file inplace | `tbl --inplace` |
| New Directory | create equivalent files in a new directory | `tbl --output-dir /path/to/dir` |
| Interactive | load dataframe in interactive python session | `tbl --df` |
| Stdout | output data to stdout | `tbl` (default behavior) |

See full list of output options [below](#tbl).

## API Reference

#### `tbl`
##### Output of `tbl -h`:

```markdown
tbl is a tool for reading and editing tabular data files

Usage: tbl has two modes
1. Summary mode: tbl [ls | schema] [SUMMARY_OPTIONS]
2. Data mode:    tbl [DATA_OPTIONS]

Get help with SUMMARY_OPTIONS using tbl [ls | schema] -h

Data mode is the default mode. DATA_OPTIONS are documented below

Optional Subcommands:
  ls      Display list of tabular files, similar to the cli `ls` command
  schema  Display table representation of each schema in the selected files

General Options:
  -h, --help                       display help message
  -V, --version                    display version

Input Options:
  [PATHS]...                       input path(s) to use
  -t, --tree                       recursively use all files in tree as inputs

Transform Options:
  -c, --columns <COLUMNS>...       select only these columns [alias --select]
      --drop <DROP>...             drop column(s)
      --with-columns <NEW_COL>...  insert columns, syntax NAME:TYPE [alias --with]
      --rename <RENAME>...         rename column(s), syntax OLD_NAME=NEW_NAME
      --cast <CAST>...             change column type(s), syntax COLUMN=TYPE
      --filter <FILTER>...         filter rows by values, syntax COLUMN=VALUE
      --sort <SORT>...             sort rows, syntax COLUMN[:desc]
      --head <HEAD>                keep only the first n rows [alias --limit]
      --tail <TAIL>                keep only the last n rows
      --offset <OFFSET>            skip the first n rows of table
      --value-counts <COLUMN>      compute value counts of column(s)

Output Options:
      --no-summary                 skip printing a summary
  -n, --n <N>                      number of rows to print in stdout, all for all
      --csv                        output data as csv
      --json                       output data as json
      --jsonl                      output data as json lines
      --hex                        encode binary columns as hex for output
      --inplace                    modify files in place
      --output-file <FILE_PATH>    write all data to a single new file
      --output-dir <DIR_PATH>      rewrite all files into this output directory
      --output-prefix <PRE-FIX>    prefix to add to output filenames
      --output-postfix <POST-FIX>  postfix to add to output filenames
      --df                         load as DataFrame in interactive python session
      --lf                         load as LazyFrame in interactive python session
      --executable <EXECUTABLE>    python executable to use with --df or --lf
      --confirm                    confirm that files should be edited
      --dry                        dry run without editing files

Output Modes:
1. output results in single file   --output-file /path/to/file.parquet
2. modify each file inplace        --inplace
3. copy files into a new dir       --output-dir /path/to/dir
4. load as interactive python      --df | --lf
5. output data to stdout           (default behavior)
```

#### `tbl ls`
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

#### `tbl schema`
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

## FAQ

### What is parquet?

[Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) is a file format for storing tabular datasets. In many cases parquet is a simpler and faster alternative to using an actual database. Parquet has become an industry standard and its ecosystem of tools is growing rapidly.

### What other parquet cli tools exist?

The most common tools are [`duckdb`](https://duckdb.org/docs/api/cli/overview), [`pqrs`](https://github.com/manojkarthick/pqrs), and [`parquet-cli`](https://github.com/apache/parquet-java/blob/master/parquet-cli/README.md).

### Why use `tbl` when `duckdb` has a cli?

`duckdb` is an incredible tool. We recommend checking it out, especially when you're running complex workloads. However there are 3 reasons you might prefer `tbl` as a cli tool:
1. **CLI-Native:** Compared to `duckdb`'s SQL, `tbl` has a cli-native syntax. This makes `tbl` simpler to use with fewer keystrokes:
    1. `duckdb "DESCRIBE read_parquet('test.parquet')"` vs `tbl schema test.parquet` 
    2. `duckdb "SELECT * FROM read_parquet('test.parquet')"` vs `tbl test.parquet`
    3. `duckdb "SELECT * FROM read_parquet('test.parquet') ORDER BY co1"` vs `tbl test.parquet --sort col1`
2. **High Level vs Low Level:** Sometimes SQL can also be a very low-level language. `tbl` and `polars` let you operate on a higher level of abstraction which reduces cognitive load:
    1. `duckdb`: `duckdb "SELECT col1, COUNT(col1) FROM read_parquet('test.parquet') GROUP BY col1"`
    2. `tbl`: `tbl test.parquet --value-counts col1`
3. **Operational QoL:** `tbl` is built specifically for making it easy to manage large parquet archives. Features like `--tree`, `--inplace`, and multi-schema commands make life easier for archive management.

### What is the plan for `tbl`?

There are a few features that we are currently exploring:
1. **S3 and cloud buckets**: ability to read and write cloud bucket parquet files using the same operations that can be performed on local files
2. **Re-partitioning**: ability to change how a set of parquet files are partitioned, such as changing the partition key or partition size
3. **Direct python syntax**: ability to directly use python polars syntax to perform complex operations like `group_by()`, `join()`, and more
4. **Idempotent Workflows**: ability to interrupt and re-run commands arbitrarily would make migrations more robust
