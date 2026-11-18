//! Execution: single-flight loads now; the speculative prefetch executor and budget governor
//! come with plan T24 and T25.

mod singleflight;

pub use singleflight::{Role, SingleFlight};
