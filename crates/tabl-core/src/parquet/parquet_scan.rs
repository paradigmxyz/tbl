use crate::TablError;
use polars::prelude::*;
use std::path::PathBuf;

/// create lazy frame by scanning input paths
pub fn create_lazyframe(paths: &[PathBuf]) -> Result<LazyFrame, TablError> {
    let scan_args = polars::prelude::ScanArgsParquet::default();
    let arc_paths = Arc::from(paths.to_vec().into_boxed_slice());
    Ok(LazyFrame::scan_parquet_files(arc_paths, scan_args)?)
}
