//! Qonduit core: protocol structs, constants, and shared types.
//!
//! This crate contains the ground truth for the Qubic wire protocol,
//! extracted from the C++ core source. All structs are `#[repr(C)]`
//! with zerocopy support for zero-decode TCP packet parsing.

pub mod constants;
pub mod epoch_intervals;
pub mod error;
pub mod event;
pub mod hash;
pub mod header;
pub mod identity;
pub mod message_type;
pub mod pipeline;
pub mod structs;

pub use constants::*;
pub use error::QonduitError;
pub use event::*;
pub use hash::*;
pub use header::RequestResponseHeader;
pub use identity::*;
pub use message_type::NetworkMessageType;
pub use pipeline::{PipelineState, PipelineStatusResponse};
pub use structs::*;
