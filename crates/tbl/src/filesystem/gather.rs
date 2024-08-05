use crate::TablError;
use futures::stream::StreamExt;
use std::path::{Path, PathBuf};

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

/// count number of existing files
pub async fn count_existing_files(paths: &[PathBuf]) -> usize {
    const CONCURRENT_LIMIT: usize = 1000; // Adjust based on your system's capabilities

    futures::stream::iter(paths)
        .map(tokio::fs::metadata)
        .buffer_unordered(CONCURRENT_LIMIT)
        .filter_map(|result| async move {
            match result {
                Ok(metadata) => Some(metadata.is_file()),
                Err(_) => None,
            }
        })
        .fold(0, |acc, is_file| async move {
            if is_file {
                acc + 1
            } else {
                acc
            }
        })
        .await
}

