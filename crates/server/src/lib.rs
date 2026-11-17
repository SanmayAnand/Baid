//! The BAiD service: HTTP API, admin API and the wiring of the engine crates.
//!
//! The `baid` binary (`main.rs`) is a thin shell over this library, so the pieces can be
//! tested directly.

pub mod auth;
pub mod cli;
pub mod config;
pub mod engine;
pub mod http;
pub mod run;
pub mod state;
