use tbl_core::TblError;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum TblCliError {
    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Error wrapper for standard IO errors.
    #[error(transparent)]
    Tbl(#[from] TblError),

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

    /// Error parsing an int
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    /// General Error
    #[error("Input error: {0}")]
    Error(String),
}

pub(crate) enum OutputMode {
    PrintToStdout,
    SaveToSingleFile,
    ModifyInplace,
    SaveToDirectory,
    Partition,
    InteractiveLf,
    InteractiveDf,
}

impl OutputMode {
    pub(crate) fn writes_to_disk(&self) -> bool {
        matches!(
            self,
            OutputMode::SaveToSingleFile
                | OutputMode::SaveToDirectory
                | OutputMode::ModifyInplace
                | OutputMode::Partition
        )
    }
}
