//! JSON-RPC 2.0 types + Komari-style methods.
//! JSON-RPC 2.0 类型与 Komari 风格的方法。
//!
//! The worker expects strict JSON-RPC 2.0 envelopes:
//! ```text
//! { "jsonrpc": "2.0", "id": <number|null>, "method": "...", "params": { ... } }
//! ```
//! Worker 要求严格的 JSON-RPC 2.0 封装。

pub mod endpoint;
pub mod http;
pub mod protocol;
pub mod proxy;
pub mod ws;

pub use http::HttpTransport;
pub use proxy::Proxy;
pub use ws::AgentClient;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON-RPC 2.0 request.
///
/// For the `report` notification the spec uses `"id": null`, so we always
/// serialize `id` even when null.
/// JSON-RPC 2.0 请求。`report` 通知要求 `id` 为 null，因此即使为空也会序列化该字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: Value, // number | string | null / 数字、字符串或 null
    pub method: String,
    pub params: Value,
}

impl RpcRequest {
    /// Construct a request with a numeric id.
    /// 创建带数字 ID 的请求。
    pub fn with_id(id: u64, method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Value::from(id),
            method: method.into(),
            params,
        }
    }

    /// Construct a notification: `"id": null`.
    /// 创建通知：`"id": null`。
    pub fn notification(method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Value::Null,
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC 2.0 response (we mostly ignore these; the server uses them
/// only for `basicReport` ack / `event` flows).
/// JSON-RPC 2.0 响应；服务端仅用于 `basicReport` 确认和 `event` 流程。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}
