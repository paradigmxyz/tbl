use crate::{TablCliError, TailArgs};

pub(crate) async fn tail_command(_args: TailArgs) -> Result<(), TablCliError> {
    println!("tail");
    Ok(())
}
