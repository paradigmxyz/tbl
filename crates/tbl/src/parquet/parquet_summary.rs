use crate::TblError;
use futures::stream::{self, StreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use polars::prelude::*;
use std::collections::HashMap;

/// get the number of rows in a parquet file
pub async fn get_parquet_row_count(path: &std::path::Path) -> Result<u64, TblError> {
    let file = tokio::fs::File::open(path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await?
        .with_batch_size(1);
    let file_metadata = builder.metadata().file_metadata();
    Ok(file_metadata.num_rows() as u64)
}

/// get the number of rows in multiple parquet files
pub async fn get_parquet_row_counts(paths: &[&std::path::Path]) -> Result<Vec<u64>, TblError> {
    let row_counts = stream::iter(paths)
        .map(|path| get_parquet_row_count(path))
        .buffered(10)
        .collect::<Vec<Result<u64, TblError>>>()
        .await;

    row_counts
        .into_iter()
        .collect::<Result<Vec<u64>, TblError>>()
}

/// get parquet schema
pub async fn get_parquet_schema(path: &std::path::Path) -> Result<Arc<Schema>, TblError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let scan_args = ScanArgsParquet::default();
        let mut lf = LazyFrame::scan_parquet(path, scan_args)?;
        let schema = lf.schema()?;
        Ok(schema)
    })
    .await?
}

/// get parquet schemas
pub async fn get_parquet_schemas(
    paths: &[std::path::PathBuf],
) -> Result<Vec<Arc<Schema>>, TblError> {
    let schemas = stream::iter(paths)
        .map(|path| get_parquet_schema(path))
        .buffered(10)
        .collect::<Vec<Result<Arc<Schema>, TblError>>>()
        .await;

    schemas
        .into_iter()
        .collect::<Result<Vec<Arc<Schema>>, TblError>>()
}

/// TabularSummary
#[derive(Clone, Default)]
pub struct TabularSummary {
    /// n_files
    pub n_files: u64,
    /// n_bytes_compressed
    pub n_bytes_compressed: u64,
    /// n_bytes_uncompressed
    pub n_bytes_uncompressed: u64,
    /// n_rows
    pub n_rows: u64,
    /// schema
    pub schema: Arc<Schema>,
    /// columns
    pub columns: Vec<TabularColumnSummary>,
}

/// TabularColumnSummary
#[derive(Default, Clone, Debug)]
pub struct TabularColumnSummary {
    /// n_bytes_compressed
    pub n_bytes_compressed: u64,
    /// n_bytes_uncompressed
    pub n_bytes_uncompressed: u64,
    // /// n_null
    // pub n_null: u64,
    // /// n_unique
    // pub n_unique: u64,
    // pub min_value
    // pub max_value
}

/// get summary of parquet file
pub async fn get_parquet_summary(path: &std::path::Path) -> Result<TabularSummary, TblError> {
    let metadata = std::fs::metadata(path)?;
    let n_bytes_compressed = metadata.len();
    let n_rows = get_parquet_row_count(path).await?;
    let schema = get_parquet_schema(path).await?;

    let parquet_metadata = get_parquet_metadata(path).await?;
    let columns = get_parquet_column_summaries(parquet_metadata.clone()).await?;
    let n_bytes_uncompressed = get_parquet_n_bytes_uncompressed(parquet_metadata);

    Ok(TabularSummary {
        n_files: 1,
        n_bytes_compressed,
        n_bytes_uncompressed,
        n_rows,
        schema,
        columns,
    })
}

/// get parquet file metadata
pub async fn get_parquet_metadata(
    path: &std::path::Path,
) -> Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>, TblError> {
    let file = tokio::fs::File::open(path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await?
        .with_batch_size(1);
    Ok(builder.metadata().clone())
}

/// get parquet uncompressed bytes
pub fn get_parquet_n_bytes_uncompressed(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
) -> u64 {
    metadata
        .row_groups()
        .iter()
        .map(|rg| rg.total_byte_size() as u64)
        .sum::<u64>()
}

/// get column summaries for parquet file
pub async fn get_parquet_column_summaries(
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
) -> Result<Vec<TabularColumnSummary>, TblError> {
    let n_columns = metadata
        .row_groups()
        .first()
        .map(|rg| rg.columns().len())
        .unwrap_or(0);
    let mut columns: Vec<TabularColumnSummary> = vec![TabularColumnSummary::default(); n_columns];
    for rg in metadata.row_groups() {
        for (column, column_metadata) in columns.iter_mut().zip(rg.columns()) {
            column.n_bytes_compressed += column_metadata.compressed_size() as u64;
            column.n_bytes_uncompressed += column_metadata.uncompressed_size() as u64;
        }
    }
    Ok(columns)
}

/// get parquet schemas
pub async fn get_parquet_summaries(
    paths: &[std::path::PathBuf],
) -> Result<Vec<TabularSummary>, TblError> {
    let schemas = stream::iter(paths)
        .map(|path| get_parquet_summary(path))
        .buffered(10)
        .collect::<Vec<Result<TabularSummary, TblError>>>()
        .await;

    schemas
        .into_iter()
        .collect::<Result<Vec<TabularSummary>, TblError>>()
}

/// combine tabular summaries
pub fn combine_tabular_summaries(
    summaries: &[&TabularSummary],
    include_columns: bool,
) -> Result<TabularSummary, TblError> {
    let mut total_summary = TabularSummary::default();
    for (s, summary) in summaries.iter().enumerate() {
        if s == 0 {
            total_summary.schema = summary.schema.clone();
        }
        total_summary.n_files += summary.n_files;
        total_summary.n_bytes_compressed += summary.n_bytes_compressed;
        total_summary.n_bytes_uncompressed += summary.n_bytes_uncompressed;
        total_summary.n_rows += summary.n_rows;
        if include_columns {
            total_summary.columns = combine_tabular_columns_summaries(
                total_summary.columns.as_slice(),
                summary.columns.as_slice(),
            )?;
        }
    }
    Ok(total_summary)
}

fn combine_tabular_columns_summaries(
    lhs: &[TabularColumnSummary],
    rhs: &[TabularColumnSummary],
) -> Result<Vec<TabularColumnSummary>, TblError> {
    if lhs.is_empty() {
        Ok(rhs.to_vec())
    } else if rhs.is_empty() {
        Ok(lhs.to_vec())
    } else if lhs.len() != rhs.len() {
        Err(TblError::SchemaError(
            "different number of columns".to_string(),
        ))
    } else {
        Ok(lhs
            .iter()
            .zip(rhs.iter())
            .map(|(lhs, rhs)| combine_tabular_column_summary(lhs, rhs))
            .collect())
    }
}

fn combine_tabular_column_summary(
    lhs: &TabularColumnSummary,
    rhs: &TabularColumnSummary,
) -> TabularColumnSummary {
    TabularColumnSummary {
        n_bytes_compressed: lhs.n_bytes_compressed + rhs.n_bytes_compressed,
        n_bytes_uncompressed: lhs.n_bytes_uncompressed + rhs.n_bytes_uncompressed,
    }
}

/// summarize by schema
pub fn summarize_by_schema(
    summaries: &[&TabularSummary],
) -> Result<HashMap<Arc<Schema>, TabularSummary>, TblError> {
    let mut by_schema: HashMap<Arc<Schema>, Vec<&TabularSummary>> = HashMap::new();
    for summary in summaries.iter() {
        by_schema
            .entry(summary.schema.clone())
            .or_default()
            .push(summary)
    }
    by_schema
        .into_iter()
        .map(|(k, v)| combine_tabular_summaries(v.as_slice(), true).map(|combined| (k, combined)))
        .collect()
}
