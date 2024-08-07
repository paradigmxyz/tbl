use crate::{DataArgs, TblCliError};
use polars::prelude::*;
use std::path::PathBuf;
use tbl::filesystem::{get_input_paths, get_output_paths, OutputPathSpec};

enum OutputMode {
    PrintToStdout,
    SaveToSingleFile,
    ModifyInplace,
    SaveToDirectory,
    Partition,
    InteractiveLf,
    InteractiveDf,
}

type InputsOutput = (Vec<PathBuf>, Option<PathBuf>);

pub(crate) async fn data_command(args: DataArgs) -> Result<(), TblCliError> {
    // decide output mode
    let output_mode = decide_output_mode(&args)?;

    // create input output pairs
    let io = gather_inputs_and_outputs(&output_mode, &args)?;

    // print data summary
    print_summary(&io, &output_mode, &args)?;

    // get user confirmation
    get_user_confirmation(&output_mode, &args)?;

    // exit early if no paths found
    if io.is_empty() {
        println!("[no tabular files selected]");
        std::process::exit(0)
    };

    // process each input output pair
    for (input_paths, output_path) in io.into_iter() {
        process_io(input_paths, output_path, &output_mode, &args)?
    }

    Ok(())
}

fn decide_output_mode(args: &DataArgs) -> Result<OutputMode, TblCliError> {
    match (
        args.inplace,
        &args.output_file,
        &args.output_dir,
        &args.partition,
        args.df,
        args.lf,
    ) {
        (false, None, None, None, false, false) => Ok(OutputMode::PrintToStdout),
        (true, None, None, None, false, false) => Ok(OutputMode::ModifyInplace),
        (false, Some(_), None, None, false, false) => Ok(OutputMode::SaveToSingleFile),
        (false, None, Some(_), None, false, false) => Ok(OutputMode::SaveToDirectory),
        (false, None, _, Some(_), false, false) => Ok(OutputMode::Partition),
        (false, None, None, None, true, false) => Ok(OutputMode::InteractiveDf),
        (false, None, None, None, false, true) => Ok(OutputMode::InteractiveLf),
        _ => Err(TblCliError::Error(
            "can only specify one output mode".to_string(),
        )),
    }
}

fn gather_inputs_and_outputs(
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<Vec<InputsOutput>, TblCliError> {
    let mut io = Vec::new();
    match output_mode {
        OutputMode::PrintToStdout
        | OutputMode::Partition
        | OutputMode::InteractiveLf
        | OutputMode::InteractiveDf => {
            let input_paths = get_input_paths(&args.paths, args.tree, true)?;
            io.push((input_paths, None))
        }
        OutputMode::SaveToSingleFile => {
            let input_paths = get_input_paths(&args.paths, args.tree, true)?;
            io.push((input_paths, args.output_file.clone()))
        }
        OutputMode::ModifyInplace => {
            let input_paths = get_input_paths(&args.paths, args.tree, true)?;
            for input_path in input_paths.into_iter() {
                io.push(([input_path.clone()].to_vec(), Some(input_path)))
            }
        }
        OutputMode::SaveToDirectory => {
            let output_spec = OutputPathSpec {
                inputs: args.paths.clone(),
                output_dir: args.output_dir.clone(),
                tree: args.tree,
                file_prefix: args.output_prefix.clone(),
                file_postfix: args.output_postfix.clone(),
                sort: true,
            };
            let (input_paths, output_paths) = get_output_paths(output_spec)?;
            for (input_path, output_path) in input_paths.into_iter().zip(output_paths) {
                io.push(([input_path].to_vec(), Some(output_path)))
            }
        }
    };

    // filter empty io pairs
    let io = io
        .into_iter()
        .filter(|(inputs, _)| !inputs.is_empty())
        .collect();

    Ok(io)
}

fn print_summary(
    inputs_and_outputs: &[InputsOutput],
    output_mode: &OutputMode,
    _args: &DataArgs,
) -> Result<(), TblCliError> {
    let mut n_input_files = 0;
    let mut _n_output_files = 0;
    for (input_files, output_file) in inputs_and_outputs.iter() {
        n_input_files += input_files.len();
        if output_file.is_some() {
            _n_output_files += 1;
        }
    }

    match output_mode {
        OutputMode::PrintToStdout => {
            println!("loading {} files and printing to stdout", n_input_files)
        }
        OutputMode::SaveToSingleFile => println!(
            "loading {} files and merging result into 1 output file",
            n_input_files
        ),
        OutputMode::SaveToDirectory => println!(
            "loading {} files and saving results to new directory",
            n_input_files
        ),
        OutputMode::ModifyInplace => println!("modifying {} files in-place", n_input_files),
        OutputMode::Partition => {}
        OutputMode::InteractiveLf => println!(
            "starting interactive session, loading {} files into LazyFrame",
            n_input_files
        ),
        OutputMode::InteractiveDf => println!(
            "starting interactive session, loading {} files into LazyFrame",
            n_input_files
        ),
    }

    Ok(())
}

fn get_user_confirmation(output_mode: &OutputMode, args: &DataArgs) -> Result<(), TblCliError> {
    if args.dry {
        println!("[dry run, exiting]");
        std::process::exit(0);
    }

    match output_mode {
        OutputMode::SaveToSingleFile | OutputMode::SaveToDirectory | OutputMode::ModifyInplace => {}
        _ => return Ok(()),
    }

    if !args.confirm {
        let prompt = "continue? ";
        if let Ok(true) = inquire::Confirm::new(prompt).with_default(false).prompt() {
            Ok(())
        } else {
            Err(TblCliError::Error("exiting".to_string()))
        }
    } else {
        Ok(())
    }
}

fn process_io(
    input_paths: Vec<PathBuf>,
    output_path: Option<PathBuf>,
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    // create lazy frame
    let lf = create_lazyframe(&input_paths)?;

    // transform into output frames
    let lf = crate::transform::apply_transformations(lf, args)?;

    // output data
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

fn create_lazyframe(paths: &[PathBuf]) -> Result<LazyFrame, TblCliError> {
    let scan_args = polars::prelude::ScanArgsParquet::default();
    let arc_paths = Arc::from(paths.to_vec().into_boxed_slice());
    Ok(LazyFrame::scan_parquet_files(arc_paths, scan_args)?)
}

//
// // output functions
//

fn print_lazyframe(lf: LazyFrame, _args: &DataArgs) -> Result<(), TblCliError> {
    // match (args.csv, args.json) {
    //     (false, false) => {}
    //     (true, false) => {}
    //     (false, true) => {}
    //     (true, true) => {}
    // };
    let df = lf.collect()?;
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
