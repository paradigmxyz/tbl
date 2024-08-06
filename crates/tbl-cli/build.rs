
use std::process::Command;

fn main() {
    // Get the most recent tag
    let tag_output = Command::new("git")
        .args(["describe", "--tags", "--abbrev=0"])
        .output()
        .expect("Failed to execute git command for tag");

    let tag = String::from_utf8(tag_output.stdout)
        .expect("Invalid UTF-8 output from git for tag")
        .trim()
        .to_string();

    // Get the git description (includes commits since tag, if any)
    let desc_output = Command::new("git")
        .args(["describe", "--always", "--dirty"])
        .output()
        .expect("Failed to execute git command for description");

    let git_description = String::from_utf8(desc_output.stdout)
        .expect("Invalid UTF-8 output from git for description")
        .trim()
        .to_string();

    // Combine tag and description
    let version_string = if tag == git_description {
        // If they're the same, just use one
        tag
    } else {
        format!("{}-{}", tag, git_description)
    };

    println!("cargo:rustc-env=GIT_DESCRIPTION={}", version_string);

    built::write_built_file().expect("Failed to acquire build-time information");
}
