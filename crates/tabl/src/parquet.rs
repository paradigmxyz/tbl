use crate::TablError;
use futures::stream::{self, StreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

/// get the number of rows in a parquet file
pub async fn get_parquet_row_count(path: &std::path::PathBuf) -> Result<u64, TablError> {
    let file = tokio::fs::File::open(path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await?
        .with_batch_size(1);
    let file_metadata = builder.metadata().file_metadata();
    Ok(file_metadata.num_rows() as u64)
}

/// get the number of rows in multiple parquet files
pub async fn get_parquet_row_counts(paths: &[std::path::PathBuf]) -> Result<Vec<u64>, TablError> {
    let row_counts = stream::iter(paths)
        .map(get_parquet_row_count)
        .buffer_unordered(10)
        .collect::<Vec<Result<u64, TablError>>>()
        .await;

    row_counts
        .into_iter()
        .collect::<Result<Vec<u64>, TablError>>()
}
