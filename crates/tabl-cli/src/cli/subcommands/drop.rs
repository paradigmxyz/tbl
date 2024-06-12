use crate::{DropArgs, TablCliError};

pub(crate) fn drop_command(_args: DropArgs) -> Result<(), TablCliError> {
    println!("drop");
    Ok(())
}
