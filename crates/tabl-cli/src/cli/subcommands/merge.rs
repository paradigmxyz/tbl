use crate::{MergeArgs, TablCliError};

pub(crate) fn merge_command(_args: MergeArgs) -> Result<(), TablCliError> {
    println!("merge");
    Ok(())
}
