use crate::styles::FontStyle;
use crate::{MergeArgs, TablCliError};

pub(crate) async fn merge_command(args: MergeArgs) -> Result<(), TablCliError> {
    inquire::set_global_render_config(crate::styles::get_render_config());

    // check inputs
    if args.inputs.len() <= 1 {
        return Err(TablCliError::Error(
            "must specify at least 2 files to merge".to_string(),
        ));
    }

    // print summary
    println!(
        "merging {} files:",
        format!("{}", args.inputs.len()).colorize_constant()
    );
    if args.inputs.len() > 10 {
        for path in args.inputs.iter().take(5) {
            println!(
                "{} {}",
                "-".colorize_title(),
                path.to_string_lossy().colorize_string()
            )
        }
        if args.inputs.len() > 10 {
            println!("...")
        }
        for path in args.inputs.iter().skip(args.inputs.len() - 5) {
            println!(
                "{} {}",
                "-".colorize_title(),
                path.to_string_lossy().colorize_string()
            )
        }
    } else {
        for path in args.inputs.iter().take(10) {
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
    tbl::parquet::merge_parquets(&args.inputs, &args.output_path, 1_000_000).await?;

    // delete old files
    if !args.keep {
        for input in args.inputs.iter() {
            std::fs::remove_file(input)?
        }
        println!(
            "{}",
            format!(
                "original {} files deleted",
                format!("{}", args.inputs.len()).colorize_constant()
            )
            .colorize_variable()
        );
    }
    println!("merge complete");

    Ok(())
}
