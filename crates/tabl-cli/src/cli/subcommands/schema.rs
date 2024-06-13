use crate::{SchemaArgs, TablCliError};
use polars::prelude::*;
use std::collections::HashMap;
use toolstr::Colorize;

pub(crate) async fn schema_command(args: SchemaArgs) -> Result<(), TablCliError> {
    // get schemas
    let paths = crate::get_file_paths(args.inputs, args.tree)?;
    let schemas = tabl::parquet::get_parquet_schemas(&paths).await?;
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
        for (path, schema) in paths.iter().zip(schemas) {
            example_paths
                .entry(schema.clone())
                .or_default()
                .push(path.clone());
        }
    }

    // print summary
    println!(
        "{} unique schemas used across {} files, showing top {} schemas below",
        format!("{}", unique_schemas.len()).green().bold(),
        format!("{}", paths.len()).green().bold(),
        format!("{}", n).green().bold(),
    );
    println!();

    // print top schemas
    let format = toolstr::NumberFormat::new().percentage().precision(2);
    let top_n = top_n_schemas(unique_schemas, n);
    let n_total_rows = 0;
    for (i, (schema, n_occurrences)) in top_n.into_iter().enumerate() {
        let file_percent = (n_occurrences as f64) / (paths.len() as f64);
        let file_percent = format.format(file_percent)?;
        let n_schema_rows = 0;
        let row_percent = if n_total_rows == 0 {
            0.0
        } else {
            (n_schema_rows as f64) / (n_total_rows as f64)
        };
        let row_percent = format.format(row_percent)?;
        println!(
            "Schema {}: used in {} rows ({}) across {} files ({})",
            format!("{}", i + 1).green().bold(),
            format!("{}", n_schema_rows).green().bold(),
            row_percent.green().bold(),
            format!("{}", n_occurrences).green().bold(),
            file_percent.green().bold(),
        );
        print_schema(schema.clone())?;

        if args.include_example_paths {
            println!();
            println!("Example paths:");
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

fn count_unique_schemas(schemas: &Vec<Arc<Schema>>) -> HashMap<Arc<Schema>, usize> {
    let mut schema_counts = HashMap::new();

    for schema in schemas {
        let counter = schema_counts.entry(Arc::clone(schema)).or_insert(0);
        *counter += 1;
    }

    schema_counts
}

fn top_n_schemas(
    schema_counts: HashMap<Arc<Schema>, usize>,
    n: usize,
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
    table.add_column("name", names)?;
    table.add_column("dtype", dtypes)?;

    // create format
    let name_column = toolstr::ColumnFormatShorthand::default().name("name");
    let dtype_column = toolstr::ColumnFormatShorthand::default().name("dtype");
    let mut format = toolstr::TableFormat {
        indent: 4,
        ..Default::default()
    };
    format.add_column(name_column);
    format.add_column(dtype_column);

    // print table
    format.print(table)?;

    Ok(())
}
