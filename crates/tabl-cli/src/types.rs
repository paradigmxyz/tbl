use tabl::TablError;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum TablCliError {
    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    Tabl(#[from] TablError),

    /// Error caused by arguments
    #[error("Argument error: {0}")]
    Arg(String),

    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    StripPrefix(#[from] std::path::StripPrefixError),

    /// Error wrapper for toolstr errors.
    #[error(transparent)]
    ToolstrError(#[from] toolstr::FormatError),

    /// Error wrapper for toolstr errors.
    #[error(transparent)]
    PolarsError(#[from] polars::prelude::PolarsError),

    /// Error caused by missing schema
    #[error("Argument error: {0}")]
    MissingSchemaError(String),

    /// General Error
    #[error("Input error: {0}")]
    Error(String),
}
