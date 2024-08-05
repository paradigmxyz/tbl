use super::super::args::MigrateArgs;
use super::*;
use crate::TablCliError;

pub(crate) async fn migrate_command(args: MigrateArgs) -> Result<(), TablCliError> {
    match args {
        MigrateArgs::Insert(args) => insert_command(args).await,
        MigrateArgs::Drop(args) => drop_command(args).await,
        MigrateArgs::Cast(args) => cast_command(args),
    }
}
