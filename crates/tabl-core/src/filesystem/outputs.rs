use crate::TablError;
use std::collections::HashMap;
use std::path::PathBuf;

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
    /// sort
    pub sort: bool,
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

    /// set sort
    pub fn sort(mut self, sort: bool) -> Self {
        self.sort = sort;
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
            let output = super::manipulate::convert_file_path(
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
                for sub_input in super::gather::get_directory_tabular_files(&input)?.into_iter() {
                    let output = super::manipulate::convert_file_path(
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
                for sub_input in super::gather::get_tree_tabular_files(&input)?.into_iter() {
                    // use relative path of tree leaf, change root to output_dir if provided
                    let new_path = if let Some(output_dir) = output_dir.clone() {
                        let relative_path = sub_input.strip_prefix(&input)?.to_path_buf();
                        output_dir.join(relative_path)
                    } else {
                        sub_input.clone()
                    };

                    // change file prefix and postfix
                    let output = super::manipulate::convert_file_path(
                        &new_path,
                        &None,
                        &output_spec.file_prefix,
                        &output_spec.file_postfix,
                    )?;

                    return_inputs.push(sub_input.clone());
                    return_outputs.push(output);
                }
            }
        } else {
            return Err(TablError::Error("".to_string()));
        };
    }

    let (return_inputs, return_outputs) = if output_spec.sort {
        // Create a vector of paired inputs and outputs
        let mut paired = return_inputs
            .into_iter()
            .zip(return_outputs)
            .collect::<Vec<_>>();

        // Sort the paired vector based on the output paths
        paired.sort_by(|a, b| a.1.cmp(&b.1));

        // Unzip the sorted paired vector back into separate input and output vectors
        paired.into_iter().unzip()
    } else {
        (return_inputs, return_outputs)
    };

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

    fn create_test_file_tree() -> Result<TempDir, TablError> {
        let temp_dir = TempDir::new()?;
        println!("Created temporary directory: {:?}", temp_dir.path());
        let root = temp_dir.path().join("root");

        fs::create_dir(&root)?;
        File::create(root.join("super_data_a.parquet"))?;
        File::create(root.join("super_data_b.parquet"))?;

        let data1 = root.join("data1");
        fs::create_dir(&data1)?;
        File::create(data1.join("data1_a.parquet"))?;
        File::create(data1.join("data1_b.parquet"))?;

        let sub_data1_1 = data1.join("sub_data1_1");
        fs::create_dir(&sub_data1_1)?;
        File::create(sub_data1_1.join("sub_data1_a.parquet"))?;
        File::create(sub_data1_1.join("sub_data1_b.parquet"))?;

        let data2 = root.join("data2");
        fs::create_dir(&data2)?;
        File::create(data2.join("data2_a.parquet"))?;
        File::create(data2.join("data2_b.parquet"))?;

        Ok(temp_dir)
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
                fn $name() -> Result<(), TablError> {
                    let test_case: TestCase = $value;
                    let mut spec = test_case.spec;

                    // Create temporary directory and add its path to inputs and output_dir
                    let temp_dir = create_test_file_tree()?;
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

                    let (_inputs, outputs) = match get_output_paths(spec) {
                        Ok((inputs, outputs)) => (inputs, outputs),
                        Err(e) => return Err(TablError::Error(format!("{}", e).to_string())),
                    };

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

                    Ok(())
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
