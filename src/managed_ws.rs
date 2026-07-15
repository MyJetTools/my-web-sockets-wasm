use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures::channel::mpsc;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent};

use super::WsError;

/// Maximum number of inbound frames buffered between the browser socket and the
/// read loop. If [`on_data`](crate::WsCallback::on_data) falls this far behind the
/// server, the connection is treated as a slow consumer and reconnected (see
/// [`WsError::SlowConsumer`]). Raise this if your feed is legitimately bursty.
const INCOMING_BUFFER_SIZE: usize = 1024;

/// A single WebSocket frame delivered to [`on_data`](crate::WsCallback::on_data).
#[derive(Debug, Clone)]
pub enum Message {
    /// A UTF-8 text frame.
    Text(String),
    /// A binary frame (received as an `ArrayBuffer` and copied into a `Vec`).
    Bytes(Vec<u8>),
}

/// The `readyState` of the underlying browser socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsState {
    /// The socket has been created; the connection is not open yet.
    Connecting,
    /// The connection is open and ready to communicate.
    Open,
    /// The connection is going through the closing handshake.
    Closing,
    /// The connection is closed (or could not be opened).
    Closed,
}

pub(super) struct ManagedWs {
    ws: web_sys::WebSocket,
    // Data frames flow through a bounded channel (backpressure). Errors / close /
    // overflow flow through a separate unbounded channel so they are never stuck
    // behind buffered data and are observed promptly by the read loop.
    data_rx: mpsc::Receiver<Message>,
    control_rx: mpsc::UnboundedReceiver<WsError>,
    _on_open: Closure<dyn FnMut()>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
}

impl ManagedWs {
    pub fn open(url: &str) -> Result<Self, JsValue> {
        let ws = web_sys::WebSocket::new(url)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let (data_tx, data_rx) = mpsc::channel::<Message>(INCOMING_BUFFER_SIZE);
        let (control_tx, control_rx) = mpsc::unbounded::<WsError>();

        let on_open = Closure::wrap(Box::new(|| {}) as Box<dyn FnMut()>);
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        let mut data_tx_msg = data_tx;
        let control_tx_msg = control_tx.clone();
        let ws_for_msg = ws.clone();
        let on_message = Closure::wrap(Box::new(move |e: MessageEvent| {
            let data = e.data();
            let msg = if let Some(text) = data.as_string() {
                Message::Text(text)
            } else if let Ok(buf) = data.dyn_into::<js_sys::ArrayBuffer>() {
                let arr = js_sys::Uint8Array::new(&buf);
                let mut bytes = vec![0u8; arr.length() as usize];
                arr.copy_to(&mut bytes);
                Message::Bytes(bytes)
            } else {
                return;
            };

            if let Err(err) = data_tx_msg.try_send(msg) {
                if err.is_full() {
                    // Consumer can't keep up: signal a slow consumer and close the
                    // socket. The read loop reconnects (and typically resubscribes).
                    let _ = control_tx_msg.unbounded_send(WsError::SlowConsumer);
                    let _ = ws_for_msg.close();
                }
                // is_disconnected() => the read loop is gone; nothing to do.
            }
        }) as Box<dyn FnMut(MessageEvent)>);
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        let control_tx_err = control_tx.clone();
        let on_error = Closure::wrap(Box::new(move |_e: Event| {
            let _ = control_tx_err.unbounded_send(WsError::ConnectionError);
        }) as Box<dyn FnMut(Event)>);
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        let control_tx_close = control_tx;
        let on_close = Closure::wrap(Box::new(move |e: CloseEvent| {
            let _ = control_tx_close.unbounded_send(WsError::Closed {
                code: e.code(),
                reason: e.reason(),
            });
        }) as Box<dyn FnMut(CloseEvent)>);
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        Ok(Self {
            ws,
            data_rx,
            control_rx,
            _on_open: on_open,
            _on_message: on_message,
            _on_error: on_error,
            _on_close: on_close,
        })
    }

    pub fn state(&self) -> WsState {
        match self.ws.ready_state() {
            web_sys::WebSocket::CONNECTING => WsState::Connecting,
            web_sys::WebSocket::OPEN => WsState::Open,
            web_sys::WebSocket::CLOSING => WsState::Closing,
            _ => WsState::Closed,
        }
    }

    pub fn ws_handle(&self) -> web_sys::WebSocket {
        self.ws.clone()
    }
}

impl Stream for ManagedWs {
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `ManagedWs` is `Unpin` (all fields are), so the safe `Pin::get_mut` works
        // and no `unsafe` is needed.
        let this = Pin::get_mut(self);

        // Control (errors / close / overflow) has priority so a close or a
        // slow-consumer signal is never stuck behind buffered data frames.
        match Pin::new(&mut this.control_rx).poll_next(cx) {
            Poll::Ready(Some(err)) => return Poll::Ready(Some(Err(err))),
            Poll::Ready(None) | Poll::Pending => {}
        }

        match Pin::new(&mut this.data_rx).poll_next(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ManagedWs {
    fn drop(&mut self) {
        // Detach all four JS callbacks BEFORE the underlying Closure values are freed.
        // Without this, a browser-fired `close` event after Drop would invoke a freed
        // closure → wasm-bindgen panics with "closure invoked recursively or after being
        // dropped" (the bug we hit with reqwasm/gloo-net 0.5/0.6, which forgets to detach
        // the close listener).
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        let _ = self.ws.close();
    }
}
