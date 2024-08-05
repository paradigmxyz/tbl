//! utilities for reading and editing tabular files

#![allow(dead_code)]
#![warn(missing_docs, unreachable_pub, unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

mod cli;
pub(crate) mod styles;
mod types;

pub(crate) use cli::*;
use types::*;

#[tokio::main]
async fn main() -> Result<(), TablCliError> {
    cli::run_cli().await
}
