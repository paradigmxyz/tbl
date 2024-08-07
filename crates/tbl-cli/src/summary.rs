use crate::{DataArgs, OutputMode, TblCliError};
use std::path::PathBuf;
use tbl::formats::{print_bullet, print_header};

pub(crate) fn print_summary(
    inputs_and_outputs: &[(Vec<PathBuf>, Option<PathBuf>)],
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    let mut n_input_files = 0;
    let mut _n_output_files = 0;
    for (input_files, output_file) in inputs_and_outputs.iter() {
        n_input_files += input_files.len();
        if output_file.is_some() {
            _n_output_files += 1;
        }
    }

    print_input_summary(n_input_files, args);
    println!();
    println!();
    print_transform_summary(args);
    println!();
    println!();
    print_output_mode_summary(n_input_files, output_mode, args);
    Ok(())
}

fn print_input_summary(n_input_files: usize, _args: &DataArgs) {
    print_header("Inputs");
    print_bullet("n_input_files", n_input_files.to_string());
    // println!("- n_input_bytes: {}", 0);
}

fn print_transform_summary(args: &DataArgs) {
    print_header("Transformations");
    let mut transforming = false;
    if let Some(with_columns) = &args.with_columns {
        print_bullet("adding columns", format!("{:?}", with_columns));
        transforming = true;
    }
    if let Some(filter) = &args.filter {
        print_bullet("filtering rows", format!("{:?}", filter));
        transforming = true;
    }
    if let Some(drop) = &args.drop {
        print_bullet("dropping columns", format!("{:?}", drop));
        transforming = true;
    }
    if let Some(cast) = &args.cast {
        print_bullet("casting types", format!("{:?}", cast));
        transforming = true;
    }
    if !transforming {
        println!("[no transformations]");
    }
}

fn print_output_mode_summary(n_input_files: usize, output_mode: &OutputMode, _args: &DataArgs) {
    print_header("Outputs");
    match output_mode {
        OutputMode::PrintToStdout => {
            print_bullet("output_mode", "PRINT_TO_STDOUT");
            let summary = format!("loading {} files and printing to stdout", n_input_files);
            print_bullet("summary", summary);
        }
        OutputMode::SaveToSingleFile => {
            print_bullet("output_mode", "SAVE_TO_ONE_FILE");
            let summary = format!(
                "loading {} files and merging result into 1 output file",
                n_input_files
            );
            print_bullet("summary", summary);
        }
        OutputMode::SaveToDirectory => {
            print_bullet("output_mode", "SAVE_TO_NEW_DIR");
            let summary = format!(
                "loading {} files and saving results to new directory",
                n_input_files
            );
            print_bullet("summary", summary);
        }
        OutputMode::ModifyInplace => {
            print_bullet("output_mode", "MODIFY_INPLACE");
            let summary = format!("modifying {} files in-place", n_input_files);
            print_bullet("summary", summary);
        }
        OutputMode::Partition => {
            print_bullet("output_mode", "REPARTITION");
            let summary = format!("repartitioning {} files", n_input_files);
            print_bullet("summary", summary);
        }
        OutputMode::InteractiveLf => {
            print_bullet("output_mode", "INTERACTIVE");
            let summary = format!(
                "starting interactive session, loading {} files into LazyFrame",
                n_input_files
            );
            print_bullet("summary", summary);
        }
        OutputMode::InteractiveDf => {
            print_bullet("output_mode", "INTERACTIVE");
            let summary = format!(
                "starting interactive session, loading {} files into LazyFrame",
                n_input_files
            );
            print_bullet("summary", summary);
        }
    }
}

//
// // general
//

// async fn print_drop_summary(
//     args: &DropArgs,
//     inputs: &[PathBuf],
//     outputs: &[PathBuf],
// ) -> Result<(), TblCliError> {
//     // print files
//     let n_show_files = 10;
//     println!("files:");
//     if args.inputs.paths.is_none() {
//         let cwd = std::env::current_dir()?;
//         for path in inputs.iter().take(n_show_files) {
//             println!(
//                 "- {}",
//                 path.strip_prefix(cwd.clone())?
//                     .to_string_lossy()
//                     .colorize_string()
//             );
//         }
//     } else {
//         for path in inputs.iter().take(n_show_files) {
//             println!("- {}", path.to_string_lossy().colorize_string());
//         }
//     }
//     if inputs.len() > n_show_files {
//         println!("...");
//     }

//     // print summary
//     let first_column = if let Some(first_column) = args.columns.first() {
//         first_column.clone()
//     } else {
//         return Err(TblCliError::Arg(
//             "must specify column(s) to drop".to_string(),
//         ));
//     };
//     let mut columns_str = first_column.colorize_variable().bold();
//     for column in args.columns.iter().skip(1) {
//         columns_str = format!("{}, {}", columns_str, column.colorize_variable().bold()).into()
//     }
//     let column_str = if args.columns.len() == 1 {
//         "column"
//     } else {
//         "columns"
//     };
//     let file_str = if inputs.len() == 1 { "file" } else { "files" };

//     let output_location = if let Some(output_dir) = args.output_dir.as_ref() {
//         format!(
//             "\nwriting outputs to directory {}",
//             output_dir.to_string_lossy().colorize_string()
//         )
//     } else {
//         ", editing files inplace".to_string()
//     };

//     println!();
//     println!(
//         "dropping {} {} from {} {}{}",
//         column_str,
//         columns_str,
//         tbl::formats::format_with_commas(inputs.len() as u64)
//             .colorize_constant()
//             .bold(),
//         file_str,
//         output_location
//     );

//     if args.output_dir.is_some() {
//         let n_existing = tbl::filesystem::count_existing_files(outputs).await;
//         if n_existing > 0 {
//             println!(
//                 "{} of the output files already exist and will be overwritten",
//                 tbl::formats::format_with_commas(n_existing as u64).colorize_constant(),
//             );
//         }
//     }

//     Ok(())
// }
