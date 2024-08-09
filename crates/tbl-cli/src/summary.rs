use crate::{DataArgs, OutputMode, TblCliError};
use std::path::{Path, PathBuf};
use tbl_core::formats::{print_bullet, print_header};

pub(crate) async fn print_summary(
    inputs_and_outputs: &[(Vec<PathBuf>, Option<PathBuf>)],
    output_mode: &OutputMode,
    args: &DataArgs,
) -> Result<(), TblCliError> {
    let mut n_input_files = 0;
    let mut all_input_files = Vec::new();
    let mut _n_output_files = 0;
    for (input_files, output_file) in inputs_and_outputs.iter() {
        n_input_files += input_files.len();
        all_input_files.extend(input_files.iter().map(|p| p.as_path()));
        if output_file.is_some() {
            _n_output_files += 1;
        }
    }

    // compute total size of input files
    let n_input_bytes = tbl_core::filesystem::get_total_bytes_of_files(&all_input_files).await?;

    print_input_summary(n_input_files, &all_input_files, n_input_bytes, args);
    println!();
    println!();
    print_transform_summary(args);
    println!();
    println!();
    print_output_mode_summary(n_input_files, output_mode, args);
    Ok(())
}

fn print_input_summary(
    n_input_files: usize,
    input_files: &[&Path],
    n_input_bytes: u64,
    _args: &DataArgs,
) {
    print_header("Inputs");
    print_bullet(
        "n_input_bytes",
        tbl_core::formats::format_bytes(n_input_bytes),
    );
    print_bullet(
        "n_input_files",
        tbl_core::formats::format_with_commas(n_input_files as u64),
    );

    let n_show_files = 10;
    for path in input_files.iter().take(n_show_files) {
        let path: String = path.to_string_lossy().to_string();
        tbl_core::formats::print_bullet_key_indent(path, 4);
    }
    if input_files.len() > n_show_files {
        tbl_core::formats::print_bullet_key_indent("...", 4);
    }
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

fn print_output_mode_summary(n_input_files: usize, output_mode: &OutputMode, args: &DataArgs) {
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
            if let Some(output_file) = &args.output_file {
                print_bullet("output_file", output_file.to_string_lossy());
            }
        }
        OutputMode::SaveToDirectory => {
            print_bullet("output_mode", "SAVE_TO_NEW_DIR");
            let summary = format!(
                "loading {} files and saving results to new directory",
                n_input_files
            );
            print_bullet("summary", summary);
            if let Some(output_dir) = &args.output_dir {
                print_bullet("output_dir", output_dir.to_string_lossy());
            }
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
