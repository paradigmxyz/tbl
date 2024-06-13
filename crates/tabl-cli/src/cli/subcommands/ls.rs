use crate::{LsArgs, TablCliError};
use toolstr::Colorize;

pub(crate) async fn ls_command(args: LsArgs) -> Result<(), TablCliError> {
    // get paths
    let paths = crate::get_file_paths(args.inputs, args.tree)?;

    // clear common prefix
    let paths = if args.absolute {
        paths
    } else {
        let common_prefix = tabl::filesystem::get_common_prefix(&paths)?;
        let mut new_paths = Vec::new();
        for path in paths {
            new_paths.push(path.strip_prefix(&common_prefix)?.to_owned())
        }
        new_paths
    };

    // get total file size
    let mut total_size: u64 = 0;
    for path in paths.iter() {
        let metadata = std::fs::metadata(path)?;
        total_size += metadata.len();
    }

    // decide number of files to print
    let n_print = match args.n {
        Some(n) => n,
        None => {
            if let Some((_, height)) = term_size::dimensions() {
                if height >= 5 {
                    height - 4
                } else {
                    1
                }
            } else {
                100
            }
        }
    };

    // print out file names or paths
    for path in paths.iter().take(n_print) {
        println!("{}", path.to_string_lossy().purple())
    }
    if n_print < paths.len() {
        println!(
            "{}",
            format!(
                "... {} files not shown",
                tabl::formats::format_with_commas((paths.len() - n_print) as u64).bold()
            )
            .truecolor(150, 150, 150)
        );
    }

    // get row counts
    let path_refs: Vec<&std::path::Path> =
        paths.iter().map(|path_buf| path_buf.as_path()).collect();
    let row_counts = tabl::parquet::get_parquet_row_counts(&path_refs).await?;

    // print total summary
    println!(
        "{} rows stored in {} across {} tabular files",
        tabl::formats::format_with_commas(row_counts.iter().sum())
            .green()
            .bold(),
        tabl::formats::format_bytes(total_size).green().bold(),
        tabl::formats::format_with_commas(paths.len() as u64)
            .green()
            .bold()
    );

    Ok(())
}
