use crate::{CountArgs, TablCliError};
use polars::prelude::*;

pub(crate) async fn count_command(args: CountArgs) -> Result<(), TablCliError> {
    println!("count");
    let paths = tbl::filesystem::get_input_paths(args.input_args.inputs, args.input_args.tree)?;
    let paths = Arc::from(paths.into_boxed_slice());
    let scan_args = polars::prelude::ScanArgsParquet::default();
    let lf = LazyFrame::scan_parquet_files(paths, scan_args)?;
    for column in args.columns.iter() {
        let sort_options = SortMultipleOptions::new().with_order_descending(true);
        let value_counts = lf
            .clone()
            .group_by(&[col(column)])
            .agg(&[col(column).count().alias("count")])
            .sort(["count"], sort_options)
            .collect()?
            .head(Some(10));
        println!("value counts for column: {}", column);
        println!("{:?}", value_counts);
    }
    Ok(())
}
