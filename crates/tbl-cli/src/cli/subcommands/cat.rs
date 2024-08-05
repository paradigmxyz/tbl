use crate::{CatArgs, TablCliError};

pub(crate) async fn cat_command(_args: CatArgs) -> Result<(), TablCliError> {
    println!("cat");
    Ok(())
}
