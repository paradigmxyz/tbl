use crate::{SchemaArgs, TablCliError};

pub(crate) fn schema_command(_args: SchemaArgs) -> Result<(), TablCliError> {
    println!("schema");
    Ok(())
}
