use crate::TablCliError;
use std::path::PathBuf;
use std::process::Command;

pub(crate) fn load_df_interactive(
    paths: Vec<PathBuf>,
    lazy: bool,
    executable: Option<String>,
) -> Result<(), TablCliError> {
    let paths: Vec<_> = paths
        .iter()
        .map(|path| format!("'{}'", path.to_string_lossy()))
        .collect();
    let paths_str = paths.join(",\n    ");

    let input_word = if paths.len() == 1 { "input" } else { "inputs" };

    let (pl_function, pl_variable, final_str, final_print) = if lazy {
        ("scan", "lf", "\\n# use `df = lf.collect()` to collect", "")
    } else {
        ("read", "df", "print(df)\\n", "\nprint(df)")
    };

    let python_code = format!(
        r#"
import polars as pl

inputs = [
    {}
]

{} = pl.{}_parquet(inputs)
print()
print('import polars as pl')
print()
print('# {}ing ' + str(len(inputs)) + ' {} into {}')
print('inputs = [...]')
print('{} = pl.{}_parquet(inputs)')
print("{}")
{}
"#,
        paths_str,
        pl_variable,
        pl_function,
        pl_function,
        input_word,
        pl_variable,
        pl_variable,
        pl_function,
        final_str,
        final_print,
    );

    let executable = if let Some(executable) = executable {
        executable
    } else {
        "ipython".to_string()
    };

    Command::new(executable)
        .arg("-i")
        .arg("-c")
        .arg(python_code)
        .spawn()?
        .wait()?;

    Ok(())
}
