use std::cell::Cell;

use super::{WsError, WsState};

/// A handle to a live WebSocket connection, handed to every
/// [`WsCallback`](crate::WsCallback) method. Use it to send frames, inspect
/// state, and signal initialization.
pub struct WsConnection {
    ws: web_sys::WebSocket,
    is_connected: Cell<bool>,
    is_initialized: Cell<bool>,
}

impl WsConnection {
    pub(super) fn new(ws: web_sys::WebSocket) -> Self {
        Self {
            ws,
            is_connected: Cell::new(true),
            is_initialized: Cell::new(false),
        }
    }

    /// A clone of the underlying browser socket. Escape hatch for low-level use —
    /// e.g. sending from code outside the callbacks (stash it in a shared cell in
    /// [`on_connected`](crate::WsCallback::on_connected)). Sending through this
    /// handle bypasses the [`is_connected`](Self::is_connected) guard, so prefer
    /// [`send_text`](Self::send_text) / [`send_bytes`](Self::send_bytes) when the
    /// send happens from inside a callback.
    pub fn ws_handle(&self) -> web_sys::WebSocket {
        self.ws.clone()
    }

    /// The current `readyState` of the underlying socket.
    pub fn state(&self) -> WsState {
        match self.ws.ready_state() {
            web_sys::WebSocket::CONNECTING => WsState::Connecting,
            web_sys::WebSocket::OPEN => WsState::Open,
            web_sys::WebSocket::CLOSING => WsState::Closing,
            _ => WsState::Closed,
        }
    }

    /// Send a UTF-8 text frame. Returns [`WsError::NotConnected`] if the
    /// connection has been marked disconnected, or [`WsError::SendFailed`] if the
    /// browser rejects the send.
    pub fn send_text(&self, text: &str) -> Result<(), WsError> {
        if !self.is_connected.get() {
            return Err(WsError::NotConnected);
        }
        self.ws
            .send_with_str(text)
            .map_err(|e| WsError::SendFailed(format!("{e:?}")))
    }

    /// Send a binary frame. Returns [`WsError::NotConnected`] if the connection has
    /// been marked disconnected, or [`WsError::SendFailed`] if the browser rejects
    /// the send.
    pub fn send_bytes(&self, data: &[u8]) -> Result<(), WsError> {
        if !self.is_connected.get() {
            return Err(WsError::NotConnected);
        }
        self.ws
            .send_with_u8_array(data)
            .map_err(|e| WsError::SendFailed(format!("{e:?}")))
    }

    /// Mark this connection for teardown. The read loop notices and reconnects.
    pub fn disconnect(&self) {
        self.is_connected.set(false);
    }

    /// Whether the connection is still logically connected (not torn down).
    pub fn is_connected(&self) -> bool {
        self.is_connected.get()
    }

    /// Signal that the app-level handshake is complete. **You must call this**
    /// (typically from [`on_data`](crate::WsCallback::on_data) when the server's
    /// init frame arrives) or the read loop drops the connection after
    /// `init_timeout` (13s) and reconnects forever. After this, a steady inbound
    /// `read_timeout` (10s) applies instead.
    pub fn mark_initialized(&self) {
        self.is_initialized.set(true);
    }

    /// Whether [`mark_initialized`](Self::mark_initialized) has been called.
    pub fn is_initialized(&self) -> bool {
        self.is_initialized.get()
    }
}
