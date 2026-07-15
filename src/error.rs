use std::fmt;

/// Errors surfaced by a WebSocket connection — both on the inbound stream and
/// from [`WsConnection`](crate::WsConnection) send calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsError {
    /// The browser reported a transport-level `error` event on the socket.
    ConnectionError,
    /// The socket was closed. Carries the WebSocket close `code` and `reason`.
    Closed {
        /// WebSocket close code (e.g. `1000` for a normal closure).
        code: u16,
        /// Human-readable close reason supplied by the peer (may be empty).
        reason: String,
    },
    /// The inbound buffer overflowed because [`on_data`](crate::WsCallback::on_data)
    /// could not keep up with the server. The connection is torn down and
    /// reconnected (backpressure — see the crate docs).
    SlowConsumer,
    /// A send was attempted while the connection was not open.
    NotConnected,
    /// The underlying browser `send()` call failed; carries the JS error text.
    SendFailed(String),
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WsError::ConnectionError => write!(f, "connection error"),
            WsError::Closed { code, reason } => {
                write!(f, "closed (code={code}, reason=\"{reason}\")")
            }
            WsError::SlowConsumer => write!(f, "slow consumer: inbound buffer overflow"),
            WsError::NotConnected => write!(f, "not connected"),
            WsError::SendFailed(err) => write!(f, "send failed: {err}"),
        }
    }
}

impl std::error::Error for WsError {}
