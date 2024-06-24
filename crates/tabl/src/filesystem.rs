use crate::TablError;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use futures::stream::StreamExt;

/// count number of existing files
pub async fn count_existing_files(paths: &[PathBuf]) -> usize {
    const CONCURRENT_LIMIT: usize = 1000;  // Adjust based on your system's capabilities

    futures::stream::iter(paths)
        .map(tokio::fs::metadata)
        .buffer_unordered(CONCURRENT_LIMIT)
        .filter_map(|result| async move {
            match result {
                Ok(metadata) => Some(metadata.is_file()),
                Err(_) => None,
            }
        })
        .fold(0, |acc, is_file| async move {
            if is_file {
                acc + 1
            } else {
                acc
            }
        })
        .await
}

/// return tabular file paths within directory
pub fn get_directory_tabular_files(dir_path: &Path) -> Result<Vec<PathBuf>, TablError> {
    let mut tabular_files = Vec::new();

    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && is_tabular_file(&path) {
            tabular_files.push(path);
        }
    }

    Ok(tabular_files)
}

/// get tabular files inside directory tree
pub fn get_tree_tabular_files(dir_path: &std::path::Path) -> Result<Vec<PathBuf>, TablError> {
    let mut tabular_files = Vec::new();
    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_tabular_file(&path) {
            tabular_files.push(path);
        } else if path.is_dir() {
            let sub_dir_files = get_tree_tabular_files(&path)?;
            tabular_files.extend(sub_dir_files);
        }
    }
    Ok(tabular_files)
}

/// return true if file_path has a tabular extension
pub fn is_tabular_file(file_path: &std::path::Path) -> bool {
    // let tabular_extensions = ["parquet", "csv"];
    let tabular_extensions = ["parquet"];

    if let Some(extension) = file_path.extension() {
        let extension = extension.to_string_lossy().to_string();
        tabular_extensions.contains(&extension.as_str())
    } else {
        false
    }
}

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

/// get file paths
pub fn get_input_paths(
    inputs: Option<Vec<PathBuf>>,
    tree: bool,
    ) -> Result<Vec<PathBuf>, TablError> {
    // get paths
    let raw_paths = match inputs {
        Some(raw_paths) => raw_paths,
        None => vec![std::env::current_dir()?],
    };

    // expand tree if specified
    let mut paths: Vec<std::path::PathBuf> = vec![];
    for raw_path in raw_paths.into_iter() {
        if raw_path.is_dir() {
            let sub_paths = if tree {
                get_tree_tabular_files(&raw_path)?
            } else {
                get_directory_tabular_files(&raw_path)?
            };
            paths.extend(sub_paths);
        } else if is_tabular_file(&raw_path) {
            paths.push(raw_path);
        } else {
            println!("skipping non-tabular file {:?}", raw_path)
        }
    }

    Ok(paths)
}

/// output path spec
#[derive(Default, Debug)]
pub struct OutputPathSpec {
    /// inputs
    pub inputs: Option<Vec<PathBuf>>,
    /// output_dir
    pub output_dir: Option<PathBuf>,
    /// tree
    pub tree: bool,
    /// file_prefix
    pub file_prefix: Option<String>,
    /// file_postfix
    pub file_postfix: Option<String>,
}

impl OutputPathSpec {
    /// create new OutputPathSpec
    pub fn new() -> Self {
        OutputPathSpec::default()
    }

    /// set inputs
    pub fn inputs<I>(mut self, inputs: I) -> Self
        where
        I: Into<InputPaths>,
        {
            self.inputs = inputs.into().0;
            self
        }

    /// set output_dir
    pub fn output_dir<T>(mut self, output_dir: T) -> Self
        where
        T: Into<OutputDirType>,
        {
            self.output_dir = output_dir.into().into();
            self
        }

    /// set tree
    pub fn tree(mut self, tree: bool) -> Self {
        self.tree = tree;
        self
    }

    /// set file_prefix
    pub fn file_prefix<T>(mut self, file_prefix: T) -> Self
        where
        T: Into<Option<String>>,
        {
            self.file_prefix = file_prefix.into();
            self
        }

    /// set file_postfix
    pub fn file_postfix<T>(mut self, file_postfix: T) -> Self
        where
        T: Into<Option<String>>,
        {
            self.file_postfix = file_postfix.into();
            self
        }
}

/// output dir type
pub enum OutputDirType {
    /// &str
    Str(&'static str),
    /// String
    String(String),
    /// PathBuf
    PathBuf(PathBuf),
    /// None
    None,
}

impl From<OutputDirType> for Option<PathBuf> {
    fn from(output_dir: OutputDirType) -> Self {
        match output_dir {
            OutputDirType::Str(s) => Some(PathBuf::from(s)),
            OutputDirType::String(s) => Some(PathBuf::from(s)),
            OutputDirType::PathBuf(p) => Some(p),
            OutputDirType::None => None,
        }
    }
}

// Implement From for all the required types
impl From<&'static str> for OutputDirType {
    fn from(s: &'static str) -> Self {
        OutputDirType::Str(s)
    }
}

impl From<String> for OutputDirType {
    fn from(s: String) -> Self {
        OutputDirType::String(s)
    }
}

impl From<PathBuf> for OutputDirType {
    fn from(p: PathBuf) -> Self {
        OutputDirType::PathBuf(p)
    }
}

impl<T> From<Option<T>> for OutputDirType
where
T: Into<OutputDirType>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => OutputDirType::None,
        }
    }
}

// New wrapper type
/// InputPaths
pub struct InputPaths(Option<Vec<PathBuf>>);

impl From<Vec<PathBuf>> for InputPaths {
    fn from(v: Vec<PathBuf>) -> Self {
        InputPaths(Some(v))
    }
}

impl From<Option<Vec<PathBuf>>> for InputPaths {
    fn from(v: Option<Vec<PathBuf>>) -> Self {
        InputPaths(v)
    }
}

impl From<Vec<String>> for InputPaths {
    fn from(v: Vec<String>) -> Self {
        InputPaths(Some(v.into_iter().map(PathBuf::from).collect()))
    }
}

impl From<Option<Vec<String>>> for InputPaths {
    fn from(v: Option<Vec<String>>) -> Self {
        InputPaths(v.map(|strings| strings.into_iter().map(PathBuf::from).collect()))
    }
}

impl<'a> From<Vec<&'a str>> for InputPaths {
    fn from(v: Vec<&'a str>) -> Self {
        InputPaths(Some(v.into_iter().map(PathBuf::from).collect()))
    }
}

impl<'a> From<Option<Vec<&'a str>>> for InputPaths {
    fn from(v: Option<Vec<&'a str>>) -> Self {
        InputPaths(v.map(|strings| strings.into_iter().map(PathBuf::from).collect()))
    }
}

/** get_output_dir() has many possible combinations of parameters

  possible dimensions of inputs
  - dimension: with or without --tree
  - dimension: with or without --output-dir
  - dimension: with or without --inputs
  - dimension: single or multiple --inputs
  - dimension: relative or absolute --inputs
  - dimension: file or directory --inputs

  cases that are easy:
  - without --inputs, without --tree, without --output-dir
  - read from CWD, write outputs to CWD
  - without --inputs, without --tree, with --output-dir
  - read from CWD, write outputs to --output-dir
  - without --inputs, with --tree, without --output-dir
  - read from CWD, write each file in its own original dir
  - without --inputs, with --tree, with --output-dir
  - read from CWD, write relative tree paths relative to --output-dir tree

  cases that are harder:
  - with single file --inputs
  - --tree doesnt matter
  - without --output-dir: writes file to that file's dir
  - with --output-dir: writes file to that dir
  - with single dir --inputs
  - without --tree, without --output-dir
  - read from the input dir, write to the input dir
  - without --tree, with --output-dir
  - read from the input dir, write to the output dir
  - with --tree, without --output-dir
  - use the input dir as tree root for both reading and writing
  - with --tree, with --output-dir
  - use input tree as reading tree root, output dir as writing tree root
  - with multiple --inputs
  - just treat each input path independently

  if --output-dir is used without --tree, every output goes directly in directory
  if --output-dir is used with --tree, the --output-dir is used as the new tree root
  */
pub fn get_output_paths(
    // inputs: Option<Vec<PathBuf>>,
    // output_dir: Option<PathBuf>,
    // tree: bool,
    output_spec: OutputPathSpec,
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), TablError> {
    // gather inputs
    let output_dir = output_spec.output_dir;
    let inputs = match output_spec.inputs {
        None => vec![std::env::current_dir()?],
        Some(inputs) => inputs,
    };

    // process each input separately
    let mut return_inputs: Vec<PathBuf> = Vec::new();
    let mut return_outputs: Vec<PathBuf> = Vec::new();
    for input in inputs {
        let metadata = std::fs::metadata(&input)?;
        if metadata.is_file() {
            // case 1: input is a file
            let output = convert_file_path(
                &input,
                &output_dir,
                &output_spec.file_prefix,
                &output_spec.file_postfix,
                )?;
            return_inputs.push(input.clone());
            return_outputs.push(output);
        } else if metadata.is_dir() {
            if !output_spec.tree {
                // case 2: input is a directory, non-tree mode
                for sub_input in get_directory_tabular_files(&input)?.into_iter() {
                    let output = convert_file_path(
                        &sub_input,
                        &output_dir,
                        &output_spec.file_prefix,
                        &output_spec.file_postfix,
                        )?;
                    return_inputs.push(sub_input);
                    return_outputs.push(output);
                }
            } else {
                // case 3: input is a directory, tree mode
                for sub_input in get_tree_tabular_files(&input)?.into_iter() {
                    // use relative path of tree leaf, change root to output_dir if provided
                    let new_path = if let Some(output_dir) = output_dir.clone() {
                        let relative_path = sub_input.strip_prefix(&input)?.to_path_buf();
                        output_dir.join(relative_path)
                    } else {
                        sub_input.clone()
                    };

                    // change file prefix and postfix
                    let output = convert_file_path(
                        &new_path,
                        &None,
                        &output_spec.file_prefix,
                        &output_spec.file_postfix,
                        )?;

                    return_inputs.push(input.clone());
                    return_outputs.push(output);
                }
            }
        } else {
            return Err(TablError::Error("".to_string()));
        };
    }

    // check that all output paths are unique to avoid collisions
    let mut count_per_output: HashMap<PathBuf, usize> = HashMap::new();
    for output in return_outputs.iter() {
        *count_per_output.entry(output.clone()).or_insert(0) += 1;
        if count_per_output[output] > 1 {
            return Err(TablError::Error(format!(
                        "Duplicate output path: {:?}",
                        output
                        )));
        }
    }

    Ok((return_inputs, return_outputs))
}

/// convert file path to new input
fn convert_file_path(
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

/// get output path
pub fn get_output_path(
    path: PathBuf,
    output_dir: Option<String>,
    root_dir: Option<String>,
    output_prefix: Option<String>,
    output_postfix: Option<String>,
    ) -> PathBuf {
    let path = Path::new(&path);

    // Calculate new directory based on `root_dir` and `output_dir`
    let new_dir = if let Some(output_dir) = output_dir {
        if let Some(root_dir) = root_dir {
            // if root_dir is specified, use the path relative to the root_dir
            PathBuf::from(output_dir).join(path.strip_prefix(Path::new(&root_dir)).unwrap_or(path))
        } else {
            // if root_dir is not specified, just use the filename
            PathBuf::from(output_dir).join(path.file_name().unwrap_or(path.as_os_str()))
        }
    } else {
        path.to_path_buf()
    };

    // Calculate new filename based on `output_prefix` and `output_postfix`
    let new_filename = {
        let mut filename = path.file_stem().unwrap_or(path.as_os_str()).to_os_string();

        // add output_prefix to filename if specified
        if let Some(output_prefix) = output_prefix {
            filename = format!("{}{}", output_prefix, filename.to_string_lossy()).into();
        }

        // add output_postfix to filename if specified
        if let Some(output_postfix) = output_postfix {
            filename = format!("{}{}", filename.to_string_lossy(), output_postfix).into();
        }

        // add file extension
        if let Some(extension) = path.extension() {
            filename.push(".");
            filename.push(extension);
        }

        filename
    };

    new_dir.join(new_filename)
}

/*
   tests
   for the tests, generate the following file tree:
   root/
   super_data_a.parquet
   super_data_b.parquet
   data1/
   data1_a.parquet
   data1_b.parquet
   sub_data1_1/
   sub_data1_a.parquet
   sub_data1_b.parquet
   data2/
   data2_a.parquet
   data2_b.parquet
   test cases:
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]).tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]).output_dir("./root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]).output_dir("./root").tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]).output_dir("./other_root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root"]).output_dir("./other_root").tree(true))

   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]).tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]).output_dir("./root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]).output_dir("./root").tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]).output_dir("./other_root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1"]).output_dir("./other_root").tree(true))

   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]).tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]).output_dir("./root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]).output_dir("./root").tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]).output_dir("./other_root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1", "./root/data2"]).output_dir("./other_root").tree(true))

   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]).tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]).output_dir("./root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]).output_dir("./root").tree(true))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]).output_dir("./other_root"))
   get_output_paths(OutputPathSpec::new().inputs(vec!["./root/data1/data1_a.parquet", "./root/super_data_a.parquet"]).output_dir("./other_root").tree(true))
   */
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use tempfile::TempDir;

    fn create_test_file_tree() -> TempDir {
        let temp_dir = TempDir::new().unwrap();
        println!("Created temporary directory: {:?}", temp_dir.path());
        let root = temp_dir.path().join("root");

        fs::create_dir(&root).unwrap();
        File::create(root.join("super_data_a.parquet")).unwrap();
        File::create(root.join("super_data_b.parquet")).unwrap();

        let data1 = root.join("data1");
        fs::create_dir(&data1).unwrap();
        File::create(data1.join("data1_a.parquet")).unwrap();
        File::create(data1.join("data1_b.parquet")).unwrap();

        let sub_data1_1 = data1.join("sub_data1_1");
        fs::create_dir(&sub_data1_1).unwrap();
        File::create(sub_data1_1.join("sub_data1_a.parquet")).unwrap();
        File::create(sub_data1_1.join("sub_data1_b.parquet")).unwrap();

        let data2 = root.join("data2");
        fs::create_dir(&data2).unwrap();
        File::create(data2.join("data2_a.parquet")).unwrap();
        File::create(data2.join("data2_b.parquet")).unwrap();

        temp_dir
    }

    struct TestCase {
        name: &'static str,
        spec: OutputPathSpec,
        expected_outputs: Vec<&'static str>,
    }

    macro_rules! generate_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let test_case: TestCase = $value;
                    let mut spec = test_case.spec;

                    // Create temporary directory and add its path to inputs and output_dir
                    let temp_dir = create_test_file_tree();
                    let temp_path = temp_dir.path().to_path_buf();

                    // Update inputs with temporary directory path
                    if let Some(inputs) = spec.inputs.as_ref() {
                        spec.inputs = Some(inputs.iter().map(|p| temp_path.join(p)).collect());
                    } else {
                        spec.inputs = Some(vec![temp_path.join("root")]);
                    }

                    // Update output_dir with temporary directory path if it exists
                    if let Some(output_dir) = spec.output_dir.as_ref() {
                        spec.output_dir = Some(temp_path.join(output_dir));
                    }

                    let (_inputs, outputs) = get_output_paths(spec).unwrap();

                    let expected_outputs: Vec<PathBuf> = test_case.expected_outputs
                        .into_iter()
                        .map(|p| temp_dir.path().join(p))
                        .collect();

                    let mut sorted_outputs = outputs.clone();
                    sorted_outputs.sort();
                    let mut sorted_expected_outputs = expected_outputs.clone();
                    sorted_expected_outputs.sort();
                    assert_eq!(
                        sorted_outputs,
                        sorted_expected_outputs,
                        "Test case '{}' failed.\nExpected (sorted): {:?}\nGot (sorted): {:?}",
                        test_case.name,
                        sorted_expected_outputs,
                        sorted_outputs
                        );
                }
            )*
        }
    }

    generate_tests! {
        test_root_input: TestCase {
            name: "Root input",
            spec: OutputPathSpec::new().inputs(vec!["root"]),
            expected_outputs: vec![
                "root/super_data_a.parquet",
                "root/super_data_b.parquet",
            ],
        },
        test_root_input_tree: TestCase {
            name: "Root input with tree",
            spec: OutputPathSpec::new().inputs(vec!["root"]).tree(true),
            expected_outputs: vec![
                "root/super_data_a.parquet",
                "root/super_data_b.parquet",
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
                "root/data1/sub_data1_1/sub_data1_a.parquet",
                "root/data1/sub_data1_1/sub_data1_b.parquet",
                "root/data2/data2_a.parquet",
                "root/data2/data2_b.parquet",
            ],
        },
        test_root_input_self_output_dir: TestCase {
            name: "Root input with self output dir",
            spec: OutputPathSpec::new().inputs(vec!["root"]).output_dir("root"),
            expected_outputs: vec![
                "root/super_data_a.parquet",
                "root/super_data_b.parquet",
            ],
        },
        test_root_input_self_output_dir_tree: TestCase {
            name: "Root input with self output dir tree",
            spec: OutputPathSpec::new().inputs(vec!["root"]).output_dir("root").tree(true),
            expected_outputs: vec![
                "root/super_data_a.parquet",
                "root/super_data_b.parquet",
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
                "root/data1/sub_data1_1/sub_data1_a.parquet",
                "root/data1/sub_data1_1/sub_data1_b.parquet",
                "root/data2/data2_a.parquet",
                "root/data2/data2_b.parquet",
            ],
        },
        test_root_input_output_dir: TestCase {
            name: "Root input with other output dir",
            spec: OutputPathSpec::new().inputs(vec!["root"]).output_dir("other_root"),
            expected_outputs: vec![
                "other_root/super_data_a.parquet",
                "other_root/super_data_b.parquet",
            ],
        },
        test_root_input_output_dir_tree: TestCase {
            name: "Root input with other output dir tree",
            spec: OutputPathSpec::new().inputs(vec!["root"]).output_dir("other_root").tree(true),
            expected_outputs: vec![
                "other_root/super_data_a.parquet",
                "other_root/super_data_b.parquet",
                "other_root/data1/data1_a.parquet",
                "other_root/data1/data1_b.parquet",
                "other_root/data1/sub_data1_1/sub_data1_a.parquet",
                "other_root/data1/sub_data1_1/sub_data1_b.parquet",
                "other_root/data2/data2_a.parquet",
                "other_root/data2/data2_b.parquet",
            ],
        },

        test_data1_input: TestCase {
            name: "Data1 input",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
            ],
        },
        test_data1_input_tree: TestCase {
            name: "Data1 input with tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]).tree(true),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
                "root/data1/sub_data1_1/sub_data1_a.parquet",
                "root/data1/sub_data1_1/sub_data1_b.parquet",
            ],
        },
        test_data1_input_root_output: TestCase {
            name: "Data1 input with root output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]).output_dir("root"),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/data1_b.parquet",
            ],
        },
        test_data1_input_root_output_tree: TestCase {
            name: "Data1 input with root output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]).output_dir("root").tree(true),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/data1_b.parquet",
                "root/sub_data1_1/sub_data1_a.parquet",
                "root/sub_data1_1/sub_data1_b.parquet",
            ],
        },
        test_data1_input_other_output: TestCase {
            name: "Data1 input with other output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]).output_dir("other_root"),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/data1_b.parquet",
            ],
        },
        test_data1_input_other_output_tree: TestCase {
            name: "Data1 input with other output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1"]).output_dir("other_root").tree(true),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/data1_b.parquet",
                "other_root/sub_data1_1/sub_data1_a.parquet",
                "other_root/sub_data1_1/sub_data1_b.parquet",
            ],
        },
        test_data1_data2_input: TestCase {
            name: "Data1 and Data2 input",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
                "root/data2/data2_a.parquet",
                "root/data2/data2_b.parquet",
            ],
        },
        test_data1_data2_input_tree: TestCase {
            name: "Data1 and Data2 input with tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]).tree(true),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/data1/data1_b.parquet",
                "root/data1/sub_data1_1/sub_data1_a.parquet",
                "root/data1/sub_data1_1/sub_data1_b.parquet",
                "root/data2/data2_a.parquet",
                "root/data2/data2_b.parquet",
            ],
        },
        test_data1_data2_input_root_output: TestCase {
            name: "Data1 and Data2 input with root output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]).output_dir("root"),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/data1_b.parquet",
                "root/data2_a.parquet",
                "root/data2_b.parquet",
            ],
        },
        test_data1_data2_input_root_output_tree: TestCase {
            name: "Data1 and Data2 input with root output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]).output_dir("root").tree(true),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/data1_b.parquet",
                "root/sub_data1_1/sub_data1_a.parquet",
                "root/sub_data1_1/sub_data1_b.parquet",
                "root/data2_a.parquet",
                "root/data2_b.parquet",
            ],
        },
        test_data1_data2_input_other_output: TestCase {
            name: "Data1 and Data2 input with other output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]).output_dir("other_root"),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/data1_b.parquet",
                "other_root/data2_a.parquet",
                "other_root/data2_b.parquet",
            ],
        },
        test_data1_data2_input_other_output_tree: TestCase {
            name: "Data1 and Data2 input with other output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1", "root/data2"]).output_dir("other_root").tree(true),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/data1_b.parquet",
                "other_root/sub_data1_1/sub_data1_a.parquet",
                "other_root/sub_data1_1/sub_data1_b.parquet",
                "other_root/data2_a.parquet",
                "other_root/data2_b.parquet",
            ],
        },
        test_specific_files_input: TestCase {
            name: "Specific files input",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/super_data_a.parquet",
            ],
        },
        test_specific_files_input_tree: TestCase {
            name: "Specific files input with tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]).tree(true),
            expected_outputs: vec![
                "root/data1/data1_a.parquet",
                "root/super_data_a.parquet",
            ],
        },
        test_specific_files_input_root_output: TestCase {
            name: "Specific files input with root output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]).output_dir("root"),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/super_data_a.parquet",
            ],
        },
        test_specific_files_input_root_output_tree: TestCase {
            name: "Specific files input with root output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]).output_dir("root").tree(true),
            expected_outputs: vec![
                "root/data1_a.parquet",
                "root/super_data_a.parquet",
            ],
        },
        test_specific_files_input_other_output: TestCase {
            name: "Specific files input with other output",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]).output_dir("other_root"),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/super_data_a.parquet",
            ],
        },
        test_specific_files_input_other_output_tree: TestCase {
            name: "Specific files input with other output and tree",
            spec: OutputPathSpec::new().inputs(vec!["root/data1/data1_a.parquet", "root/super_data_a.parquet"]).output_dir("other_root").tree(true),
            expected_outputs: vec![
                "other_root/data1_a.parquet",
                "other_root/super_data_a.parquet",
            ],
        },

    }
}
