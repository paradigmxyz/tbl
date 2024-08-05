use crate::{CastArgs, TablCliError};

pub(crate) fn cast_command(_args: CastArgs) -> Result<(), TablCliError> {
    println!("cast");
    Ok(())
}
