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

    /// Error wrapper for tokio errors.
    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// Error wrapper for schema errors.
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// Error wrapper for input errors.
    #[error("Input error: {0}")]
    InputError(String),
}
