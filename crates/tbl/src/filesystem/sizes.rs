use futures::stream::{FuturesUnordered, StreamExt};
use std::path::Path;
use tokio::fs;

/// get total number of bytes across files
pub async fn get_total_bytes_of_files(file_paths: Vec<&Path>) -> Result<u64, std::io::Error> {
    let futures = file_paths.into_iter().map(|path| async move {
        let metadata = fs::metadata(path).await?;
        Ok::<u64, std::io::Error>(if metadata.is_file() {
            metadata.len()
        } else {
            0
        })
    });

    let mut total: u64 = 0;
    let mut futures: FuturesUnordered<_> = futures.collect();
    while let Some(result) = futures.next().await {
        total += result?;
    }

    Ok(total)
}
