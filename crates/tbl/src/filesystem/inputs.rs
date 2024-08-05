use crate::TablError;
use std::path::PathBuf;

/// get file paths
pub fn get_input_paths(
    inputs: Option<Vec<PathBuf>>,
    tree: bool,
    sort: bool,
) -> Result<Vec<PathBuf>, TablError> {
    // get paths
    let raw_paths = match inputs {
        Some(raw_paths) => raw_paths,
        None => vec![std::env::current_dir()?],
    };

    // expand tree if specified
    let mut paths: Vec<std::path::PathBuf> = vec![];
    for raw_path in raw_paths.into_iter() {
        if raw_path.is_dir() {
            let sub_paths = if tree {
                super::gather::get_tree_tabular_files(&raw_path)?
            } else {
                super::gather::get_directory_tabular_files(&raw_path)?
            };
            paths.extend(sub_paths);
        } else if super::gather::is_tabular_file(&raw_path) {
            paths.push(raw_path);
        } else {
            println!("skipping non-tabular file {:?}", raw_path)
        }
    }

    // sort
    if sort {
        paths.sort()
    }

    Ok(paths)
}
