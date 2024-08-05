use crate::styles::FontStyle;
use crate::{MergeArgs, TablCliError};

pub(crate) async fn merge_command(args: MergeArgs) -> Result<(), TablCliError> {
    inquire::set_global_render_config(crate::styles::get_render_config());

    let paths = tbl::filesystem::get_input_paths(args.inputs.paths, args.inputs.tree)?;

    // check inputs
    if paths.len() <= 1 {
        return Err(TablCliError::Error(
            "must specify at least 2 files to merge".to_string(),
        ));
    }

    // print summary
    println!(
        "merging {} files:",
        format!("{}", paths.len()).colorize_constant()
    );
    if paths.len() > 10 {
        for path in paths.iter().take(5) {
            println!(
                "{} {}",
                "-".colorize_title(),
                path.to_string_lossy().colorize_string()
            )
        }
        if paths.len() > 10 {
            println!("...")
        }
        for path in paths.iter().skip(paths.len() - 5) {
            println!(
                "{} {}",
                "-".colorize_title(),
                path.to_string_lossy().colorize_string()
            )
        }
    } else {
        for path in paths.iter().take(10) {
            println!(
                "{} {}",
                "-".colorize_title(),
                path.to_string_lossy().colorize_string()
            )
        }
    }
    println!(
        "output file: {}",
        args.output_path.to_string_lossy().colorize_string()
    );
    if args.keep {
        println!("{}", "(will NOT delete original files)".colorize_variable())
    } else {
        println!("{}", "(WILL delete original files)".colorize_variable())
    }

    // get confirmation to edit files
    if !args.confirm {
        let prompt = "continue? ";
        if let Ok(true) = inquire::Confirm::new(prompt).with_default(false).prompt() {
        } else {
            return Ok(());
        }
    }

    // merge files
    tbl::parquet::merge_parquets(&paths, &args.output_path, 1_000_000).await?;

    // delete old files
    if !args.keep {
        for input in paths.iter() {
            std::fs::remove_file(input)?
        }
        println!(
            "{}",
            format!(
                "original {} files deleted",
                format!("{}", paths.len()).colorize_constant()
            )
            .colorize_variable()
        );
    }
    println!("merge complete");

    Ok(())
}
