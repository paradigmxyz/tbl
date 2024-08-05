use crate::TablError;
use std::path::{Component, Path, PathBuf};

/// get common prefix of paths
pub fn get_common_prefix(paths: &[PathBuf]) -> Result<PathBuf, TablError> {
    if paths.is_empty() {
        return Err(TablError::InputError("no paths given".to_string()));
    }

    let mut components_iter = paths.iter().map(|p| p.components());
    let mut common_components: Vec<Component<'_>> = components_iter
        .next()
        .expect("There should be at least one path")
        .collect();

    for components in components_iter {
        common_components = common_components
            .iter()
            .zip(components)
            .take_while(|(a, b)| a == &b)
            .map(|(a, _)| *a)
            .collect();
    }

    Ok(common_components.iter().collect())
}

/// convert file path to new input
pub fn convert_file_path(
    input: &Path,
    output_dir: &Option<PathBuf>,
    file_prefix: &Option<String>,
    file_postfix: &Option<String>,
) -> Result<PathBuf, TablError> {
    // change output directory
    let output = match output_dir.as_ref() {
        Some(output_dir) => {
            let file_name = input
                .file_name()
                .ok_or_else(|| TablError::Error("Invalid input path".to_string()))?;
            output_dir.join(file_name)
        }
        None => input.to_path_buf(),
    };

    if file_prefix.is_some() || file_postfix.is_some() {
        let stem = output
            .file_stem()
            .ok_or_else(|| TablError::Error("Invalid output path".to_string()))?;
        let extension = output.extension();

        let new_filename = format!(
            "{}{}{}{}",
            file_prefix.as_deref().unwrap_or(""),
            stem.to_string_lossy(),
            file_postfix.as_deref().unwrap_or(""),
            extension.map_or_else(String::new, |ext| format!(".{}", ext.to_string_lossy()))
        );

        Ok(output.with_file_name(new_filename))
    } else {
        Ok(output)
    }
}
