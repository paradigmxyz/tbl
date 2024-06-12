use crate::{LsArgs, TablCliError};
use std::path::{Component, PathBuf};
use toolstr::Colorize;

pub(crate) async fn ls_command(args: LsArgs) -> Result<(), TablCliError> {
    // get paths
    let raw_paths = match args.inputs {
        Some(raw_paths) => raw_paths,
        None => vec![std::env::current_dir()?],
    };

    // get common prefix
    let common_prefix = if args.absolute {
        None
    } else {
        Some(get_common_prefix(&raw_paths)?)
    };

    // expand tree if specified
    let mut paths: Vec<std::path::PathBuf> = vec![];
    for raw_path in raw_paths.into_iter() {
        if raw_path.is_dir() {
            let sub_paths = if args.tree {
                tabl::filesystem::get_tree_tabular_files(&raw_path)?
            } else {
                tabl::filesystem::get_directory_tabular_files(&raw_path)?
            };

            if let Some(common_prefix) = &common_prefix {
                for path in sub_paths {
                    paths.push(path.strip_prefix(common_prefix)?.to_owned())
                }
            } else {
                for path in sub_paths {
                    paths.push(path)
                }
            };
        } else if tabl::filesystem::is_tabular_file(&raw_path) {
            paths.push(raw_path);
        } else {
            println!("skipping non-tabular file {:?}", raw_path)
        }
    }

    // get total file size
    let mut total_size: u64 = 0;
    for path in paths.iter() {
        let metadata = std::fs::metadata(path)?;
        total_size += metadata.len();
    }

    // decide number to print
    let n_print = match args.n {
        Some(n) => n,
        None => {
            if let Some((_, height)) = term_size::dimensions() {
                if height >= 5 {
                    height - 4
                } else {
                    1
                }
            } else {
                100
            }
        }
    };

    for path in paths.iter().take(n_print) {
        println!("{}", path.to_string_lossy().purple())
    }
    if n_print < paths.len() {
        println!(
            "{}",
            format!(
                "... {} files not shown",
                format_with_commas((paths.len() - n_print) as u64).bold()
            )
            .truecolor(150, 150, 150)
        );
    }

    // get row counts
    let row_counts = tabl::parquet::get_parquet_row_counts(&paths).await?;

    println!(
        "{} rows stored using {} across {} tabular files",
        format_with_commas(row_counts.iter().sum()).green().bold(),
        format_bytes(total_size).green().bold(),
        format_with_commas(paths.len() as u64).green().bold()
    );

    Ok(())
}

fn get_common_prefix(paths: &[PathBuf]) -> Result<PathBuf, TablCliError> {
    if paths.is_empty() {
        return Err(TablCliError::Arg("no paths given".to_string()));
    }

    let mut components_iter = paths.iter().map(|p| p.components());
    let mut common_components: Vec<Component<'_>> = components_iter
        .next()
        .expect("There should be at least one path")
        .collect();

    for components in components_iter {
        common_components = common_components
            .iter()
            .zip(components)
            .take_while(|(a, b)| a == &b)
            .map(|(a, _)| *a)
            .collect();
    }

    Ok(common_components.iter().collect())
}

fn format_bytes(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut size = bytes as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < units.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    format!("{:.2} {}", size, units[unit])
}

fn format_with_commas(number: u64) -> String {
    let num_str = number.to_string();
    let mut result = String::new();
    let mut count = 0;

    for c in num_str.chars().rev() {
        if count == 3 {
            result.push(',');
            count = 0;
        }
        result.push(c);
        count += 1;
    }

    result.chars().rev().collect()
}
