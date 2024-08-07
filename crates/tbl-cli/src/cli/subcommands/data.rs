use crate::{DataArgs, OutputMode, TblCliError};
use std::path::PathBuf;
use tbl::filesystem::{get_input_paths, get_output_paths, OutputPathSpec};

pub(crate) async fn data_command(args: DataArgs) -> Result<(), TblCliError> {
    inquire::set_global_render_config(crate::styles::get_render_config());

    // decide output mode
    let output_mode = decide_output_mode(&args)?;

    // create input output pairs
    let io = gather_inputs_and_outputs(&output_mode, &args)?;

    // print data summary
    if !args.no_summary {
        crate::summary::print_summary(&io, &output_mode, &args).await?;
    }

    // exit early as needed
    exit_early_if_needed(args.dry, args.confirm, &output_mode, &io);

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

#[allow(clippy::type_complexity)]
fn gather_inputs_and_outputs(
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<Vec<(Vec<PathBuf>, Option<PathBuf>)>, TblCliError> {
    // parse input output pairs
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

fn exit_early_if_needed(
    dry: bool,
    confirm: bool,
    output_mode: &OutputMode,
    io: &[(Vec<PathBuf>, Option<PathBuf>)],
) {
    // exit if performing dry run
    if dry {
        println!("[dry run, exiting]");
        std::process::exit(0);
    }

    // exit if no files selected
    if io.is_empty() {
        println!("[no tabular files selected]");
        std::process::exit(0)
    };

    // exit if user does not confirm write operations
    if output_mode.writes_to_disk() & !confirm {
        let prompt = "continue? ";
        if let Ok(true) = inquire::Confirm::new(prompt).with_default(false).prompt() {
        } else {
            println!("[exiting]");
            std::process::exit(0)
        }
    }
}

fn process_io(
    input_paths: Vec<PathBuf>,
    output_path: Option<PathBuf>,
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    // create lazy frame
    let lf = tbl::parquet::create_lazyframe(&input_paths)?;

    // transform into output frames
    let lf = crate::transform::apply_transformations(lf, args)?;

    // output data
    crate::output::output_lazyframe(lf, input_paths, output_path, output_mode, args)
}
