use crate::TablError;
use futures::stream::{self, StreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use polars::prelude::*;

/// get the number of rows in a parquet file
pub async fn get_parquet_row_count(path: &std::path::Path) -> Result<u64, TablError> {
    let file = tokio::fs::File::open(path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await?
        .with_batch_size(1);
    let file_metadata = builder.metadata().file_metadata();
    Ok(file_metadata.num_rows() as u64)
}

/// get the number of rows in multiple parquet files
pub async fn get_parquet_row_counts(paths: &[&std::path::Path]) -> Result<Vec<u64>, TablError> {
    let row_counts = stream::iter(paths)
        .map(|path| get_parquet_row_count(path))
        .buffered(10)
        .collect::<Vec<Result<u64, TablError>>>()
        .await;

    row_counts
        .into_iter()
        .collect::<Result<Vec<u64>, TablError>>()
}

/// get parquet schema
pub async fn get_parquet_schema(path: &std::path::Path) -> Result<Arc<Schema>, TablError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let scan_args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(path, scan_args)?;
        let schema = lf.schema()?;
        Ok(schema)
    })
    .await?
}

/// get parquet schemas
pub async fn get_parquet_schemas(
    paths: &[std::path::PathBuf],
) -> Result<Vec<Arc<Schema>>, TablError> {
    let schemas = stream::iter(paths)
        .map(|path| get_parquet_schema(path))
        .buffered(10)
        .collect::<Vec<Result<Arc<Schema>, TablError>>>()
        .await;

    schemas
        .into_iter()
        .collect::<Result<Vec<Arc<Schema>>, TablError>>()
}

/// TabularFileSummaryOptions
#[derive(Default)]
pub struct TabularFileSummaryOptions {
    /// n_bytes
    pub n_bytes: bool,
    /// n_rows
    pub n_rows: bool,
    /// schema
    pub schema: bool,
    /// columns
    pub columns: bool,
}

/// TabularFileSummary
pub struct TabularFileSummary {
    /// n_bytes
    pub n_bytes: Option<u64>,
    /// n_rows
    pub n_rows: Option<u64>,
    /// schema
    pub schema: Option<Arc<Schema>>,
    /// columns
    pub columns: Option<TabularColumnSummary>,
}

/// TabularColumnSummary
pub struct TabularColumnSummary {
    /// n_bytes
    pub n_bytes: Option<u64>,
    /// n_null
    pub n_null: Option<u64>,
    /// n_unique
    pub n_unique: Option<u64>,
    // min_value
    // max_value
}

/// get summary of parquet file
pub async fn get_parquet_summary(
    path: &std::path::Path,
    options: &TabularFileSummaryOptions,
) -> Result<TabularFileSummary, TablError> {
    let n_bytes = if options.n_bytes {
        let metadata = std::fs::metadata(path)?;
        Some(metadata.len())
    } else {
        None
    };

    let n_rows = if options.n_rows {
        Some(get_parquet_row_count(path).await?)
    } else {
        None
    };

    let schema = if options.schema {
        Some(get_parquet_schema(path).await?)
    } else {
        None
    };

    // let columns = if options.columns { None } else { None };
    let columns = None;

    Ok(TabularFileSummary {
        n_bytes,
        n_rows,
        schema,
        columns,
    })
}

/// get parquet schemas
pub async fn get_parquet_summaries(
    paths: &[std::path::PathBuf],
    options: TabularFileSummaryOptions,
) -> Result<Vec<TabularFileSummary>, TablError> {
    let schemas = stream::iter(paths)
        .map(|path| get_parquet_summary(path, &options))
        .buffered(10)
        .collect::<Vec<Result<TabularFileSummary, TablError>>>()
        .await;

    schemas
        .into_iter()
        .collect::<Result<Vec<TabularFileSummary>, TablError>>()
}
