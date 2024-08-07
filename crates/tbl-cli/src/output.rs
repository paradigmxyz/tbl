use crate::{DataArgs, OutputMode, TblCliError};
use polars::prelude::*;
use std::path::PathBuf;

pub(crate) fn output_lazyframe(
    lf: LazyFrame,
    input_paths: Vec<PathBuf>,
    output_path: Option<PathBuf>,
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    match output_mode {
        OutputMode::PrintToStdout => print_lazyframe(lf, args),
        OutputMode::SaveToSingleFile => save_lf_to_disk(lf, output_path, args),
        OutputMode::SaveToDirectory => save_lf_to_disk(lf, output_path, args),
        OutputMode::ModifyInplace => save_lf_to_disk(lf, output_path, args),
        OutputMode::Partition => partition_data(lf, input_paths, args),
        OutputMode::InteractiveLf => enter_interactive_session(lf, input_paths, args),
        OutputMode::InteractiveDf => enter_interactive_session(lf, input_paths, args),
    }
}

fn print_lazyframe(lf: LazyFrame, _args: &DataArgs) -> Result<(), TblCliError> {
    // match (args.csv, args.json) {
    //     (false, false) => {}
    //     (true, false) => {}
    //     (false, true) => {}
    //     (true, true) => {}
    // };
    let df = lf.collect()?;
    println!();
    println!();
    tbl::formats::print_header("Data");
    println!("{}", df);
    Ok(())
}

fn save_lf_to_disk(
    lf: LazyFrame,
    output_path: Option<PathBuf>,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    let output_path = match output_path {
        Some(output_path) => output_path,
        None => return Err(TblCliError::Error("no output path specified".to_string())),
    };
    if output_path.ends_with(".csv") | args.csv {
        let options = CsvWriterOptions::default();
        lf.sink_csv(output_path, options)?;
    } else if output_path.ends_with(".json") | args.json {
        let options = JsonWriterOptions::default();
        lf.sink_json(output_path, options)?;
    } else {
        let options = ParquetWriteOptions::default();
        lf.sink_parquet(output_path, options)?;
    };
    Ok(())
}

fn partition_data(
    _lf: LazyFrame,
    _input_paths: Vec<PathBuf>,
    _args: &DataArgs,
) -> Result<(), TblCliError> {
    Err(TblCliError::Error(
        "partition functionality not implemented".to_string(),
    ))
}

fn enter_interactive_session(
    _lf: LazyFrame,
    input_paths: Vec<PathBuf>,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    crate::python::load_df_interactive(input_paths, args.lf, args.executable.clone())
}
