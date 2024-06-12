use crate::{PartitionArgs, TablCliError};

pub(crate) fn partition_command(_args: PartitionArgs) -> Result<(), TablCliError> {
    println!("partition");
    Ok(())
}
