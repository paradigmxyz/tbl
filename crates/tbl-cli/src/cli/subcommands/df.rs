use crate::{DfArgs, TablCliError};

pub(crate) fn df_command(args: DfArgs) -> Result<(), TablCliError> {
    let paths = tbl::filesystem::get_input_paths(args.inputs, args.tree)?;
    super::lf::load_df_interactive(paths, args.lazy)?;
    Ok(())
}
