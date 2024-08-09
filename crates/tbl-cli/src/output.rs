use crate::styles::FontStyle;
use crate::{DataArgs, OutputMode, TblCliError};
use color_print::cstr;
use polars::prelude::*;
use std::io::stdout;
use std::path::PathBuf;
use toolstr::Colorize;

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

fn print_lazyframe(lf: LazyFrame, args: &DataArgs) -> Result<(), TblCliError> {
    let df = lf.collect()?;

    let mut df = match args.hex {
        true => binary_to_hex(&mut df.clone())?,
        false => df,
    };

    if !args.no_summary {
        println!();
        println!();
        tbl_core::formats::print_header("Data");
    };

    let n_show = match &args.n {
        Some(n) if n == "all" => df.height(),
        Some(n) => n.parse::<usize>()?,
        None => 20,
    };
    let n_missing = if df.height() >= n_show {
        df.height() - n_show
    } else {
        0
    };

    if args.csv {
        let df = binary_to_hex(&mut df)?;
        print_dataframe_as_csv(&df, n_show)?;
    } else if args.json | args.jsonl {
        let df = binary_to_hex(&mut df)?;
        print_dataframe_as_json(&df, n_show, args.jsonl)?;
    } else {
        let df = df.head(Some(n_show));
        println!("{}", df);
    };

    if n_missing > 0 {
        println!(
            "{} rows omitted, use {} to show all rows",
            n_missing.to_string().colorize_constant().bold(),
            cstr!("<white><bold>-n all</bold></white>")
        );
    }

    Ok(())
}

fn print_dataframe_as_csv(df: &DataFrame, n: usize) -> Result<(), PolarsError> {
    let mut writer = CsvWriter::new(stdout());
    let df: DataFrame = df.head(Some(n));
    writer.finish(&mut df.clone())
}

fn print_dataframe_as_json(df: &DataFrame, n: usize, jsonl: bool) -> Result<(), PolarsError> {
    let mut writer = JsonWriter::new(stdout());

    if !jsonl {
        writer = writer.with_json_format(polars::prelude::JsonFormat::Json);
    };

    let df: DataFrame = df.head(Some(n));
    let result = writer.finish(&mut df.clone());

    if !jsonl {
        println!()
    };

    result
}

fn binary_to_hex(df: &mut DataFrame) -> Result<DataFrame, PolarsError> {
    let mut df = df.clone();

    let binary_columns: Vec<String> = df
        .get_columns()
        .iter()
        .filter_map(|s| {
            if matches!(s.dtype(), DataType::Binary) {
                Some(s.name().to_string())
            } else {
                None
            }
        })
        .collect();

    for col_name in binary_columns {
        let hex_col_with_prefix = df
            .clone()
            .lazy()
            .select(&[
                concat_str([lit("0x"), col(&col_name).binary().hex_encode()], "", true)
                    .alias(&col_name),
            ])
            .collect()?
            .column(&col_name)?
            .clone();

        df = df.with_column(hex_col_with_prefix)?.clone();
    }

    Ok(df)
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
