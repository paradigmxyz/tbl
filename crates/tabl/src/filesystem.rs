use crate::TablError;
use std::path::{Component, Path, PathBuf};

/// return tabular file paths within directory
pub fn get_directory_tabular_files(dir_path: &Path) -> Result<Vec<PathBuf>, TablError> {
    let mut tabular_files = Vec::new();

    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && is_tabular_file(&path) {
            tabular_files.push(path);
        }
    }

    Ok(tabular_files)
}

/// get tabular files inside directory tree
pub fn get_tree_tabular_files(dir_path: &std::path::Path) -> Result<Vec<PathBuf>, TablError> {
    let mut tabular_files = Vec::new();
    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_tabular_file(&path) {
            tabular_files.push(path);
        } else if path.is_dir() {
            let sub_dir_files = get_tree_tabular_files(&path)?;
            tabular_files.extend(sub_dir_files);
        }
    }
    Ok(tabular_files)
}

/// return true if file_path has a tabular extension
pub fn is_tabular_file(file_path: &std::path::Path) -> bool {
    // let tabular_extensions = ["parquet", "csv"];
    let tabular_extensions = ["parquet"];

    if let Some(extension) = file_path.extension() {
        let extension = extension.to_string_lossy().to_string();
        tabular_extensions.contains(&extension.as_str())
    } else {
        false
    }
}

/// get common prefix of paths
pub fn get_common_prefix(paths: &[PathBuf]) -> Result<PathBuf, TablError> {
    if paths.is_empty() {
        return Err(TablError::InputError("no paths given".to_string()));
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
