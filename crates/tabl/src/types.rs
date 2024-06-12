use thiserror::Error;

/// Tabl Error
#[derive(Error, Debug)]
pub enum TablError {
    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// Error wrapper for polars errors.
    #[error(transparent)]
    PolarsError(#[from] polars::prelude::PolarsError),

    /// Error wrapper for parquet errors.
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),
}
