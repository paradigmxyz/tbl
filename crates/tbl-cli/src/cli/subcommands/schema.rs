use crate::styles::FontStyle;
use crate::{SchemaArgs, TablCliError};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tbl::formats::{format_bytes, format_with_commas};
use tbl::parquet::{combine_tabular_summaries, summarize_by_schema, TabularSummary};
use toolstr::Colorize;

pub(crate) async fn schema_command(args: SchemaArgs) -> Result<(), TablCliError> {
    // get schemas
    let paths = tbl::filesystem::get_input_paths(&args.paths, args.tree, true)?;
    let summaries = tbl::parquet::get_parquet_summaries(&paths).await?;
    let ref_summaries: Vec<&tbl::parquet::TabularSummary> = summaries.iter().collect();
    let by_schema = summarize_by_schema(ref_summaries.as_slice())?;

    // summarize entire set
    let total_summary = combine_tabular_summaries(&ref_summaries, false)?;

    // clear common prefix
    let paths = if args.absolute {
        paths
    } else {
        let common_prefix = tbl::filesystem::get_common_prefix(&paths)?;
        let mut new_paths = Vec::new();
        for path in paths {
            new_paths.push(path.strip_prefix(&common_prefix)?.to_owned())
        }
        new_paths
    };

    // collect example paths for each schema
    let n_example_paths = 3;
    let example_paths = if args.examples {
        let mut example_paths = HashMap::<Arc<Schema>, Vec<PathBuf>>::new();
        for (path, summary) in paths.iter().zip(summaries.iter()) {
            example_paths
                .entry(Arc::clone(&summary.schema))
                .or_default()
                .push(path.clone());
        }
        Some(example_paths)
    } else {
        None
    };

    // decide how many schemas to show
    let n_to_show = std::cmp::min(args.n.unwrap_or(3), by_schema.len());

    // decide what to sort by
    let sort_by = match args.sort.as_str() {
        "rows" => SortSchemasBy::Rows,
        "bytes" => SortSchemasBy::Bytes,
        "files" => SortSchemasBy::Files,
        _ => {
            return Err(TablCliError::Arg(
                "must sort by rows, bytes, or files".to_string(),
            ))
        }
    };

    // print output
    print_schemas(
        by_schema,
        total_summary,
        n_to_show,
        sort_by,
        n_example_paths,
        example_paths,
    )?;

    Ok(())
}

fn count_unique_schemas(schemas: &Vec<&Arc<Schema>>) -> HashMap<Arc<Schema>, usize> {
    let mut schema_counts = HashMap::new();

    for schema in schemas {
        let counter = schema_counts.entry(Arc::clone(schema)).or_insert(0);
        *counter += 1;
    }

    schema_counts
}

pub(crate) enum SortSchemasBy {
    Files,
    Bytes,
    Rows,
}

impl std::fmt::Display for SortSchemasBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SortSchemasBy::Files => "files",
            SortSchemasBy::Bytes => "bytes",
            SortSchemasBy::Rows => "rows",
        };
        write!(f, "{}", s)
    }
}

fn top_n_schemas(
    schema_summaries: HashMap<Arc<Schema>, TabularSummary>,
    n_to_show: usize,
    sort_by: SortSchemasBy,
) -> Vec<TabularSummary> {
    let mut summaries: Vec<_> = schema_summaries.values().cloned().collect();
    match sort_by {
        SortSchemasBy::Rows => summaries.sort_by(|a, b| b.n_rows.cmp(&a.n_rows)),
        SortSchemasBy::Files => summaries.sort_by(|a, b| b.n_files.cmp(&a.n_files)),
        SortSchemasBy::Bytes => {
            summaries.sort_by(|a, b| b.n_bytes_compressed.cmp(&a.n_bytes_compressed))
        }
    }
    summaries.into_iter().take(n_to_show).collect()
}

fn print_schemas(
    schema_summaries: HashMap<Arc<Schema>, TabularSummary>,
    total_summary: TabularSummary,
    n_to_show: usize,
    sort_by: SortSchemasBy,
    n_example_paths: usize,
    example_paths: Option<HashMap<Arc<Schema>, Vec<PathBuf>>>,
) -> Result<(), TablCliError> {
    let n_schemas = schema_summaries.len();

    // print summary
    let schema_word = if n_schemas == 1 { "schema" } else { "schemas" };
    println!(
        "{} unique {}, {} rows, {} files, {}",
        format_with_commas(n_schemas as u64).green().bold(),
        schema_word,
        format_with_commas(total_summary.n_rows).green().bold(),
        format_with_commas(total_summary.n_files).green().bold(),
        format_bytes(total_summary.n_bytes_compressed)
            .green()
            .bold(),
    );
    println!();
    if n_schemas > 1 {
        println!(
            "showing top {} schemas by number of {}:",
            format!("{}", n_to_show).green().bold(),
            sort_by,
        );
        println!();
        if example_paths.is_some() {
            println!();
        };
    }

    // print top schemas
    let format = toolstr::NumberFormat::new().percentage().precision(2);
    let top_n = top_n_schemas(schema_summaries, n_to_show, sort_by);
    for (i, summary) in top_n.into_iter().enumerate() {
        let file_percent = (summary.n_files as f64) / (total_summary.n_files as f64);
        let file_percent = format.format(file_percent)?;

        let row_percent = if total_summary.n_rows == 0 {
            0.0
        } else {
            (summary.n_rows as f64) / (total_summary.n_rows as f64)
        };
        let row_percent = format.format(row_percent)?;

        let byte_percent = if total_summary.n_bytes_compressed == 0 {
            0.0
        } else {
            (summary.n_bytes_compressed as f64) / (total_summary.n_bytes_compressed as f64)
        };
        let byte_percent = format.format(byte_percent)?;

        if n_schemas > 1 {
            println!(
                "{} {}{} {} rows ({}), {} files ({}), {} ({})",
                "Schema".colorize_title(),
                format!("{}", i + 1).green().bold(),
                ":".colorize_title(),
                format_with_commas(summary.n_rows).green().bold(),
                row_percent.green().bold(),
                format_with_commas(summary.n_files).green().bold(),
                file_percent.green().bold(),
                format_bytes(summary.n_bytes_compressed).green().bold(),
                byte_percent.green().bold(),
            );
            println!();
        }
        print_schema(summary.schema.clone(), &summary)?;

        println!();
        if let Some(example_paths) = example_paths.as_ref() {
            if let Some(paths_vec) = example_paths.get(&summary.schema) {
                if n_example_paths == 1 {
                    println!("{}", "Example path".colorize_title());
                } else {
                    println!("{}", "Example paths".colorize_title());
                };
                for (i, path) in paths_vec.iter().take(n_example_paths).enumerate() {
                    println!(
                        "{} {}",
                        format!("{}.", i + 1).colorize_variable(),
                        path.to_string_lossy().colorize_comment()
                    );
                }
            }
        }

        if i < n_to_show - 1 {
            println!();
            if example_paths.is_some() {
                println!();
            }
        }
    }
    if n_to_show < n_schemas {
        println!();
        println!(
            "{} more schemas not shown",
            format!("{}", n_schemas - n_to_show).bold().green()
        )
    }

    Ok(())
}

fn print_schema(schema: Arc<Schema>, summary: &TabularSummary) -> Result<(), TablCliError> {
    // gather data
    let names: Vec<String> = schema.iter_names().map(|x| x.to_string()).collect();
    let dtypes: Vec<String> = schema.iter_dtypes().map(|x| x.to_string()).collect();
    let uncompressed: Vec<_> = summary
        .columns
        .iter()
        .map(|x| format_bytes(x.n_bytes_uncompressed))
        .collect();
    let compressed: Vec<_> = summary
        .columns
        .iter()
        .map(|x| format_bytes(x.n_bytes_compressed))
        .collect();

    let total_disk_bytes: u64 = summary.columns.iter().map(|x| x.n_bytes_compressed).sum();
    let percent_disk: Vec<_> = summary
        .columns
        .iter()
        .map(|x| {
            format!(
                "{:.2}%",
                100.0 * (x.n_bytes_compressed as f64) / (total_disk_bytes as f64)
            )
        })
        .collect();

    // build table
    let mut table = toolstr::Table::new();
    table.add_column("column name", names)?;
    table.add_column("dtype", dtypes)?;
    table.add_column("full size", uncompressed)?;
    table.add_column("disk size", compressed)?;
    table.add_column("disk %", percent_disk)?;

    // create format
    let mut name_column = toolstr::ColumnFormatShorthand::default().name("column name");
    let mut dtype_column = toolstr::ColumnFormatShorthand::default().name("dtype");
    let mut uncompressed_column = toolstr::ColumnFormatShorthand::default().name("full size");
    let mut compressed_column = toolstr::ColumnFormatShorthand::default().name("disk size");
    let mut disk_percent_column = toolstr::ColumnFormatShorthand::default().name("disk %");
    name_column.font_style = Some("".colorize_function().into());
    dtype_column.font_style = Some("".colorize_variable().into());
    uncompressed_column.font_style = Some("".colorize_constant().into());
    compressed_column.font_style = Some("".colorize_constant().into());
    disk_percent_column.font_style = Some("".colorize_constant().into());

    let mut format = toolstr::TableFormat {
        // indent: 4,
        label_font_style: Some("".colorize_title().into()),
        border_font_style: Some("".colorize_comment().into()),
        ..Default::default()
    };
    format.add_column(name_column);
    format.add_column(dtype_column);
    format.add_column(compressed_column);
    format.add_column(uncompressed_column);
    format.add_column(disk_percent_column);

    // print table
    format.print(table)?;

    Ok(())
}
