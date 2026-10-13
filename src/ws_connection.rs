use std::cell::{Cell, RefCell};

use super::{WsError, WsState};

/// A handle to a live WebSocket connection, handed to every
/// [`WsCallback`](crate::WsCallback) method. Use it to send frames, inspect
/// state, and signal initialization.
pub struct WsConnection {
    ws: web_sys::WebSocket,
    is_connected: Cell<bool>,
    is_initialized: Cell<bool>,
    disconnect_reason: RefCell<Option<WsError>>,
}

impl WsConnection {
    pub(super) fn new(ws: web_sys::WebSocket) -> Self {
        Self {
            ws,
            is_connected: Cell::new(true),
            is_initialized: Cell::new(false),
            disconnect_reason: RefCell::new(None),
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

    /// Tears the connection down because the socket itself ended, and keeps what it
    /// ended with for [`disconnect_reason`](Self::disconnect_reason).
    pub(super) fn disconnect_because(&self, reason: WsError) {
        *self.disconnect_reason.borrow_mut() = Some(reason);
        self.is_connected.set(false);
    }

    /// What ended this connection, when it was the socket that ended: the close the
    /// server sent ([`WsError::Closed`] with its code and reason), a transport error
    /// ([`WsError::ConnectionError`]) or an inbound overflow ([`WsError::SlowConsumer`]).
    /// Read it in [`on_disconnected`](crate::WsCallback::on_disconnected) to tell, say,
    /// a `4401 invalid token` from a plain network drop.
    ///
    /// `None` while the connection is alive — and when it was this side that tore it
    /// down: an init or read timeout, an `Err(())` returned by a callback,
    /// [`disconnect`](Self::disconnect), or
    /// [`WebSocketClient::stop`](crate::WebSocketClient::stop).
    pub fn disconnect_reason(&self) -> Option<WsError> {
        self.disconnect_reason.borrow().clone()
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
