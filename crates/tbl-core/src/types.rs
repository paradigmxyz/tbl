use thiserror::Error;

/// Tbl Error
#[derive(Error, Debug)]
pub enum TblError {
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

    /// Error wrapper for tokio errors.
    #[error(transparent)]
    StripPrefixError(#[from] std::path::StripPrefixError),

    /// Error wrapper for tokio errors.
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Error wrapper for schema errors.
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// Error wrapper for input errors.
    #[error("Input error: {0}")]
    InputError(String),

    /// General Error
    #[error("Input error: {0}")]
    Error(String),

    /// Error wrapper for AcquireError
    #[error(transparent)]
    TokioAcquireError(#[from] tokio::sync::AcquireError),
}
