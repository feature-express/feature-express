#![allow(clippy::boxed_local)]
#![allow(clippy::assign_op_pattern)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::new_without_default)]
#![allow(clippy::from_over_into)]
#![allow(clippy::type_complexity)]
#![warn(clippy::unwrap_used)]
#![allow(unused_macros)]
#![cfg_attr(test, allow(warnings))]

extern crate pest;
extern crate pest_derive;

#[cfg(test)]
use rstest_reuse;

#[macro_use]
mod macros;
#[macro_use]
pub mod map;
#[macro_use]
pub mod sstring;
mod aggr;
pub mod algo;
pub mod ast;
mod dataframe;
mod datetime_utils;
pub mod errors;
mod eval;
pub mod evaluation;
pub mod event;
pub mod event_index;
pub mod event_store;
pub mod feature_matrix;
mod features;
pub mod impls;
pub mod interval;
pub mod naive_aggregate_funcs;
pub mod obs_dates;
mod parser;
mod partial_agg;
mod partial_aggregates;
mod stats;
pub mod tests;
mod types;
pub mod utils;
pub mod value;
pub mod vec1;
