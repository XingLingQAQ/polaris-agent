//! Library entry point — exposes protocol + sysstat for integration tests.
//! 库入口，向集成测试公开协议和系统统计模块。

pub mod commands;
pub mod cli;
pub mod management;
pub mod config;
mod storage;
pub mod probes;
pub mod probe_addons;
pub mod probe_network;
pub mod queue;
pub mod rpc;
pub mod runtime_config;
pub mod restart;
pub mod shutdown;
pub mod sysstat;
pub mod updater;
pub mod util;

pub use queue::FrameQueue;
pub use rpc::protocol;
pub use rpc::RpcRequest;
pub use sysstat::AgentExtras;
pub use sysstat::FinalizedAggregate;
pub use sysstat::InterfaceStats;
pub use sysstat::MountStats;
pub use sysstat::Sampler;
pub use sysstat::SysSnapshot;
