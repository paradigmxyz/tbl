
# tabl

CLI utility for reading and editing parquet files

## Goals
- read or write multiple files using a single command
- use general polars syntax for modifying files

## Commands

#### Reading commands
- `tabl ls` ls files with row counts, page counts
- `tabl schema [<FILES>]` print schema of files
- `tabl stats <FILES> [--per-column]` show per-column stats
    - stats: type, size, null count, min, max

### Editing commands
- `tabl drop <COLUMNS> <FILE>` delete columns
    - `tabl drop col1 file1`
    - `tabl drop --columns col1 --inputs file1`
- `tabl cast <COLUMNS>` cast columns into new types
    - `tabl cast label=String`
- `tabl merge <FILES>` merge files into one file
- `tabl partition` partition files
- `tabl pl <POLARS_EXPRESSION>` edit using python polars expression syntax
    - `tabl pl df.group_by('name').agg(pl.first('date'))`

### Editing command options
- `--confirm` perform edits without confirmation
- `--dry` do not edit, only print out proposed edits
- `--multiple-schemas` allow input files to have non-matching schemas
- output locations
    - `--inplace` edit files in place, this is the default option
    - `--print` print output instead of writing to file
    - `--print-rows` set number of rows to print
    - `--output-file FILE_PATH` put all edited data into this file
    - `--output-directory DIR_PATH` put all new files into this directory
    - `--output-tree SOURCE_ROOT_PATH TARGET_ROOT_PATH` put all new files into this tree

# Other tools
- https://github.com/manojkarthick/pqrs
- https://github.com/rdblue/parquet-cli
- https://github.com/apache/parquet-java/blob/master/parquet-cli/README.md

# Maybe TODO
- allow opening editor to create polars expressions
- allow reading from s3

