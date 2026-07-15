use std::rc::Rc;

use super::{Message, WsConnection};

/// Application hooks for a [`WebSocketClient`](crate::WebSocketClient). Implement
/// this to provide the URL and react to the connection lifecycle.
///
/// Returning `Err(())` from [`on_connected`](Self::on_connected) or
/// [`on_data`](Self::on_data) tells the client to disconnect and reconnect — use
/// it for things like token refresh or a deliberate cool-down after a server
/// error frame.
///
/// `async fn` in the trait is intentional: this client is single-threaded (WASM),
/// so the returned futures don't need to be `Send`.
#[allow(async_fn_in_trait)]
pub trait WsCallback: 'static {
    /// The URL to connect to. Called on **every** (re)connect attempt, so it can
    /// be dynamic. Return `None` to stay idle and retry later (e.g. before login,
    /// when there is no token yet); the client keeps polling this until it returns
    /// a URL, which lets a socket "arm itself" once a token becomes available.
    fn get_url(&self) -> Option<String>;

    /// The socket reached the OPEN state. Send your initial frames and/or capture
    /// the [`ws_handle`](WsConnection::ws_handle) here. Returning `Err(())` forces
    /// a disconnect + reconnect.
    async fn on_connected(&self, conn: Rc<WsConnection>) -> Result<(), ()>;

    /// One inbound frame. Runs inline in the read loop, so keep it fast (offload
    /// heavy work with `spawn`) — a slow `on_data` can overflow the bounded inbound
    /// buffer and trip a [`WsError::SlowConsumer`](crate::WsError::SlowConsumer)
    /// reconnect. You must call [`WsConnection::mark_initialized`] here once the
    /// server's init frame arrives. Returning `Err(())` forces a disconnect +
    /// reconnect.
    async fn on_data(&self, conn: Rc<WsConnection>, msg: Message) -> Result<(), ()>;

    /// The connection closed (any reason). Clean up here.
    async fn on_disconnected(&self, conn: Rc<WsConnection>);
}
