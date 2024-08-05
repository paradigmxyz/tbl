use crate::styles::FontStyle;
use crate::{InsertArgs, TablCliError};
use arrow::datatypes::DataType;
use std::path::PathBuf;

pub(crate) async fn insert_command(args: InsertArgs) -> Result<(), TablCliError> {
    inquire::set_global_render_config(crate::styles::get_render_config());

    // parse inputs
    let output_path_spec = tbl::filesystem::OutputPathSpec::new()
        .inputs(args.inputs.paths.clone())
        .output_dir(args.output_dir.clone())
        .tree(args.inputs.tree)
        .sort(true)
        .file_prefix(args.output_prefix.clone())
        .file_postfix(args.output_postfix.clone());
    let (inputs, outputs) = tbl::filesystem::get_output_paths(output_path_spec)?;
    let (column_names, column_dtypes) = parse_user_dtypes(args.new_columns.clone())?;

    // print summary
    print_summary(&args, &inputs, &column_names, column_dtypes.as_slice())?;

    // get confirmation to edit files
    if !args.confirm {
        let prompt = "continue? ";
        if let Ok(true) = inquire::Confirm::new(prompt).with_default(false).prompt() {
        } else {
            return Ok(());
        }
    }

    // edit files
    tbl::parquet::insert_parquets_columns(
        &inputs,
        &outputs,
        column_names,
        column_dtypes,
        args.default,
        args.index,
        1_000_000,
        10,
    )
    .await?;

    // print summary
    println!("insertion complete");

    Ok(())
}

fn parse_user_dtypes(input: Vec<String>) -> Result<(Vec<String>, Vec<DataType>), TablCliError> {
    if input.len() % 2 != 0 {
        return Err(TablCliError::Error(
            "Input vector must have an even number of elements".to_string(),
        ));
    }

    let mut column_names = Vec::with_capacity(input.len() / 2);
    let mut column_dtypes = Vec::with_capacity(input.len() / 2);

    for chunk in input.chunks(2) {
        let name = chunk[0].clone();
        let dtype = match chunk[1].to_lowercase().as_str() {
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint32" => DataType::UInt32,
            "uint64" => DataType::UInt64,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "i32" => DataType::Int32,
            "i64" => DataType::Int64,
            "u32" => DataType::UInt32,
            "u64" => DataType::UInt64,
            "f32" => DataType::Float32,
            "f64" => DataType::Float64,
            "string" | "utf8" => DataType::Utf8,
            "binary" => DataType::Binary,
            "bool" | "boolean" => DataType::Boolean,
            _ => {
                return Err(TablCliError::Error(format!(
                    "Unsupported data type: {}",
                    chunk[1]
                )))
            }
        };

        column_names.push(name);
        column_dtypes.push(dtype);
    }

    Ok((column_names, column_dtypes))
}

fn format_path(
    path: &std::path::Path,
    relpath: &Option<std::path::PathBuf>,
) -> Result<String, TablCliError> {
    let path = if let Some(relpath) = relpath {
        path.strip_prefix(relpath)?
    } else {
        path
    };
    Ok(path.to_string_lossy().colorize_string().to_string())
}

fn print_summary(
    args: &InsertArgs,
    inputs: &[PathBuf],
    column_names: &[String],
    column_dtypes: &[DataType],
) -> Result<(), TablCliError> {
    // print summary
    let column_word = if column_names.len() == 1 {
        "column"
    } else {
        "columns"
    };
    let file_word = if inputs.len() == 1 { "file" } else { "files" };
    println!(
        "inserting {} {} into {} {}",
        format!("{}", column_names.len()).colorize_constant(),
        column_word,
        format!("{}", inputs.len()).colorize_constant(),
        file_word
    );
    let mut column_str = format!(
        "{} ({})",
        column_names[0].colorize_constant(),
        format!("{}", column_dtypes[0]).colorize_constant()
    );
    for (column_name, column_dtype) in column_names.iter().zip(column_dtypes.iter()).skip(1) {
        column_str = format!(
            "{}, {} ({})",
            column_str,
            column_name.colorize_constant(),
            format!("{}", column_dtype).colorize_constant()
        )
    }
    println!("columns: {}", column_str);
    let relpath = if args.inputs.paths.is_none() {
        Some(std::env::current_dir()?)
    } else {
        None
    };
    if inputs.len() == 1 {
        println!("file: {}", inputs[0].to_string_lossy().colorize_string());
    } else {
        println!("files:");
        if inputs.len() <= 10 {
            for input in inputs.iter() {
                println!("{} {}", "-".colorize_title(), format_path(input, &relpath)?);
            }
        } else {
            for input in inputs.iter().take(5) {
                println!("{} {}", "-".colorize_title(), format_path(input, &relpath)?);
            }
            println!("...");
            for input in inputs.iter().skip(inputs.len() - 5) {
                println!("{} {}", "-".colorize_title(), format_path(input, &relpath)?);
            }
        }
    }
    if let Some(output_dir) = args.output_dir.as_ref() {
        let colored = output_dir.to_string_lossy().colorize_string();
        println!(
            "{}",
            format!("writing outputs to {}", colored).colorize_variable()
        )
    } else {
        println!("{}", "editing files in place".colorize_variable())
    }

    Ok(())
}
