use crate::{SchemaArgs, TablCliError};
use polars::prelude::*;
use std::collections::HashMap;
use toolstr::Colorize;

pub(crate) async fn schema_command(args: SchemaArgs) -> Result<(), TablCliError> {
    // get schemas
    let paths = crate::get_file_paths(args.inputs, args.tree)?;
    let n_paths = paths.len() as u64;
    let options = tabl::parquet::TabularFileSummaryOptions {
        n_bytes: true,
        n_rows: true,
        schema: true,
        ..Default::default()
    };
    let summaries = tabl::parquet::get_parquet_summaries(&paths, options).await?;
    let schemas: Vec<&Arc<Schema>> = summaries
        .iter()
        .map(|summary| {
            summary
                .schema
                .as_ref()
                .ok_or(TablCliError::MissingSchemaError("h".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let unique_schemas = count_unique_schemas(&schemas);
    let n_schemas = unique_schemas.len();

    // clear common prefix
    let paths = if args.absolute {
        paths
    } else {
        let common_prefix = tabl::filesystem::get_common_prefix(&paths)?;
        let mut new_paths = Vec::new();
        for path in paths {
            new_paths.push(path.strip_prefix(&common_prefix)?.to_owned())
        }
        new_paths
    };

    // decide how many schemas to show
    let n = args.n_schemas.unwrap_or(3);
    let n = std::cmp::min(n, unique_schemas.len());

    // collect example paths for each schema
    let n_example_paths = 3;
    let mut example_paths = HashMap::<std::sync::Arc<Schema>, Vec<std::path::PathBuf>>::new();
    if args.include_example_paths {
        for (path, schema) in paths.iter().zip(schemas.iter()) {
            example_paths
                .entry(Arc::clone(schema))
                .or_default()
                .push(path.clone());
        }
    }

    // summarize n_rows and n_bytes for each schema
    let mut bytes_per_schema = HashMap::<std::sync::Arc<Schema>, u64>::new();
    let mut rows_per_schema = HashMap::<std::sync::Arc<Schema>, u64>::new();
    for (schema, summary) in schemas.iter().zip(summaries.iter()) {
        if let Some(n_bytes) = summary.n_bytes {
            *bytes_per_schema.entry(Arc::clone(schema)).or_default() += n_bytes;
        }
        if let Some(n_rows) = summary.n_rows {
            *rows_per_schema.entry(Arc::clone(schema)).or_default() += n_rows;
        }
    }

    let n_total_bytes: u64 = summaries.iter().filter_map(|summary| summary.n_bytes).sum();
    let n_total_rows: u64 = summaries.iter().filter_map(|summary| summary.n_rows).sum();

    // print summary
    let schema_word = if n_schemas == 1 { "schema" } else { "schemas" };
    println!(
        "{} unique {} for {} rows and {} files in {}",
        tabl::formats::format_with_commas(unique_schemas.len() as u64)
            .green()
            .bold(),
        schema_word,
        tabl::formats::format_with_commas(n_total_rows as u64)
            .green()
            .bold(),
        tabl::formats::format_with_commas(n_paths).green().bold(),
        tabl::formats::format_bytes(n_total_bytes).green().bold(),
    );
    println!();
    if n_schemas > 1 {
        println!(
            "showing top {} schemas below",
            format!("{}", n).green().bold(),
        );
        println!();
        if args.include_example_paths {
            println!();
        };
    }

    // print top schemas
    let format = toolstr::NumberFormat::new().percentage().precision(2);
    let sort_by = SortSchemasBy::Bytes;
    let top_n = top_n_schemas(
        unique_schemas,
        &bytes_per_schema,
        &rows_per_schema,
        n,
        sort_by,
    );
    for (i, (schema, n_occurrences)) in top_n.into_iter().enumerate() {
        let file_percent = (n_occurrences as f64) / (n_paths as f64);
        let file_percent = format.format(file_percent)?;

        let n_schema_rows = *rows_per_schema.get(&schema).unwrap_or(&0);
        let row_percent = if n_total_rows == 0 {
            0.0
        } else {
            (n_schema_rows as f64) / (n_total_rows as f64)
        };
        let row_percent = format.format(row_percent)?;

        let n_schema_bytes = *bytes_per_schema.get(&schema).unwrap_or(&0);
        let byte_percent = if n_total_bytes == 0 {
            0.0
        } else {
            (n_schema_bytes as f64) / (n_total_bytes as f64)
        };
        let byte_percent = format.format(byte_percent)?;

        if n_schemas > 1 {
            println!(
                "Schema {}: {} rows ({}) across {} files ({}) using {} ({})",
                format!("{}", i + 1).green().bold(),
                tabl::formats::format_with_commas(n_schema_rows)
                    .green()
                    .bold(),
                row_percent.green().bold(),
                tabl::formats::format_with_commas(n_occurrences as u64)
                    .green()
                    .bold(),
                file_percent.green().bold(),
                tabl::formats::format_bytes(n_schema_bytes).green().bold(),
                byte_percent.green().bold(),
            );
        }
        print_schema(schema.clone())?;

        if args.include_example_paths {
            println!();
            if n_example_paths == 1 {
                println!("Example path:");
            } else {
                println!("Example paths:");
            };
            if let Some(paths_vec) = example_paths.get(&schema) {
                for (i, path) in paths_vec.iter().take(n_example_paths).enumerate() {
                    println!("    {}. {}", i + 1, path.to_string_lossy());
                }
            }
        }

        if i < n - 1 {
            println!();
            if args.include_example_paths {
                println!();
            }
        }
    }
    if n < n_schemas {
        println!();
        println!(
            "{} more schemas not shown",
            format!("{}", n_schemas - n).bold().green()
        )
    }

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

enum SortSchemasBy {
    Files,
    Bytes,
    Rows,
}

fn top_n_schemas(
    schema_counts: HashMap<Arc<Schema>, usize>,
    _bytes_per_schema: &HashMap<std::sync::Arc<Schema>, u64>,
    _rows_per_schema: &HashMap<std::sync::Arc<Schema>, u64>,
    n: usize,
    _sort_by: SortSchemasBy,
) -> Vec<(Arc<Schema>, usize)> {
    let mut counts_vec: Vec<(Arc<Schema>, usize)> = schema_counts.into_iter().collect();
    counts_vec.sort_by(|a, b| b.1.cmp(&a.1));
    counts_vec.into_iter().take(n).collect()
}

fn print_schema(schema: std::sync::Arc<Schema>) -> Result<(), TablCliError> {
    // build data
    let names: Vec<String> = schema.iter_names().map(|x| x.to_string()).collect();
    let dtypes: Vec<String> = schema.iter_dtypes().map(|x| x.to_string()).collect();
    let mut table = toolstr::Table::new();
    table.add_column("column name", names)?;
    table.add_column("dtype", dtypes)?;

    // create format
    let mut name_column = toolstr::ColumnFormatShorthand::default().name("column name");
    let mut dtype_column = toolstr::ColumnFormatShorthand::default().name("dtype");
    name_column.font_style = Some("".blue().into());
    dtype_column.font_style = Some("".yellow().into());
    let mut format = toolstr::TableFormat {
        indent: 4,
        label_font_style: Some("".purple().bold().into()),
        ..Default::default()
    };
    format.add_column(name_column);
    format.add_column(dtype_column);

    // print table
    format.print(table)?;

    Ok(())
}
