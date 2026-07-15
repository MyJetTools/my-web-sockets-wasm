//! # my-web-sockets-wasm
//!
//! Auto-reconnecting **browser (WASM)** WebSocket client. It wraps the native
//! JavaScript `web_sys::WebSocket`, adapts its four DOM events into a
//! `futures::Stream`, and drives a reconnect + timeout loop on the **Dioxus**
//! runtime.
//!
//! Implement [`WsCallback`] to supply the URL and handle lifecycle events, then
//! drive it with [`WebSocketClient`]:
//!
//! ```ignore
//! let client = WebSocketClient::new();
//! client.start(std::rc::Rc::new(my_callback)); // must run inside a Dioxus scope
//! ```
//!
//! ## Must run inside a Dioxus runtime
//!
//! [`WebSocketClient::start`] uses `dioxus::prelude::spawn`, so it must be called
//! from within a Dioxus reactive context (a component body or a spawned task).
//!
//! ## You must call [`WsConnection::mark_initialized`]
//!
//! The read loop drops any connection that is not "initialized" within
//! `init_timeout` (13s). Call [`WsConnection::mark_initialized`] once your
//! app-level handshake completes (typically from [`WsCallback::on_data`]), or the
//! socket is force-reconnected every ~13 seconds. See the README for details.
//!
//! ## Backpressure
//!
//! Inbound frames are buffered in a bounded channel. If [`WsCallback::on_data`]
//! cannot keep up and the buffer overflows, the connection is treated as a slow
//! consumer, torn down, and reconnected (surfacing [`WsError::SlowConsumer`]).
//! Keep `on_data` fast and offload heavy work with `spawn`.

mod error;
mod managed_ws;
mod ws_callback;
mod ws_client;
mod ws_connection;

pub use error::WsError;
pub use managed_ws::{Message, WsState};
pub use ws_callback::WsCallback;
pub use ws_client::WebSocketClient;
pub use ws_connection::WsConnection;
