use crate::{StatsArgs, TablCliError};

pub(crate) fn stats_command(_args: StatsArgs) -> Result<(), TablCliError> {
    println!("stats");
    Ok(())
}
