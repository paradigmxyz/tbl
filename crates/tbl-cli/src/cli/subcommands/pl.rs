use crate::{PlArgs, TablCliError};

pub(crate) fn pl_command(_args: PlArgs) -> Result<(), TablCliError> {
    println!("pl");
    Ok(())
}
