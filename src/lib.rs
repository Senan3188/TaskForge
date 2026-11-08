#![doc = include_str!("../README.md")]

mod client;
mod dashboard;
mod master;
mod protocol;
mod worker;

pub use client::Client;
pub use master::Master;
pub use protocol::{JobState, JobStatus, SubmitJob};
pub use worker::{TaskRegistry, Worker};
