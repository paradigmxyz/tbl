use crate::TablCliError;
use std::path::PathBuf;

pub(crate) fn get_file_paths(
    inputs: Option<Vec<PathBuf>>,
    tree: bool,
) -> Result<Vec<PathBuf>, TablCliError> {
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
                tabl::filesystem::get_tree_tabular_files(&raw_path)?
            } else {
                tabl::filesystem::get_directory_tabular_files(&raw_path)?
            };
            paths.extend(sub_paths);
        } else if tabl::filesystem::is_tabular_file(&raw_path) {
            paths.push(raw_path);
        } else {
            println!("skipping non-tabular file {:?}", raw_path)
        }
    }

    Ok(paths)
}
