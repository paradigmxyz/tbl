use crate::{HeadArgs, TablCliError};

pub(crate) async fn head_command(_args: HeadArgs) -> Result<(), TablCliError> {
    println!("head");
    Ok(())
}
