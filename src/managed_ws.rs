use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, Stream, StreamExt, pin_mut, select_biased};
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

/// Why a socket did not become a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ConnectError {
    /// The socket was refused, or closed before it opened — with what the browser
    /// reported about it.
    Failed(WsError),
    /// The socket neither opened nor failed within the connect timeout.
    TimedOut,
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::Failed(err) => write!(f, "connect failed: {err}"),
            ConnectError::TimedOut => write!(f, "connect timeout"),
        }
    }
}

/// Decides how an attempt to open a socket ended, out of the three things that can
/// happen to it: it opened, it failed, or the time ran out.
///
/// `opened` is looked at **first**, and that order is the point. A socket can open and
/// be closed again before this is polled — a server that refuses right after the
/// handshake, or one whose whole answer is a few frames and a close. Such a socket did
/// connect: it gets `on_connected`, the frames it brought and `on_disconnected` like any
/// other connection. Looking at the failure first would turn it into a socket that
/// "never connected", drop its frames and report nothing to the application.
async fn connect_outcome(
    opened: impl Future<Output = ()>,
    failed: impl Future<Output = WsError>,
    timed_out: impl Future<Output = ()>,
) -> Result<(), ConnectError> {
    let opened = opened.fuse();
    let failed = failed.fuse();
    let timed_out = timed_out.fuse();
    pin_mut!(opened, failed, timed_out);

    select_biased! {
        _ = opened => Ok(()),
        err = failed => Err(ConnectError::Failed(err)),
        _ = timed_out => Err(ConnectError::TimedOut),
    }
}

/// What the socket has received, in the order it is handed to the read loop.
///
/// Data frames flow through a bounded channel (backpressure). What ends a connection —
/// an error, a close, an overflow — flows through a separate unbounded one, so that it
/// can never be lost behind a full buffer. The two are merged back here:
///
/// - The **end** of the socket (`Closed`, `ConnectionError`) is handed over *after* every
///   frame that arrived before it. A server that sends its last frames and closes at
///   once — an error message and a close, a short answer and a close — has all of them
///   delivered.
/// - A **slow-consumer** signal is handed over *ahead of* the buffered frames. By then
///   the buffer holds more than the consumer could take and a frame has already been
///   dropped: the connection is to be torn down and re-synced, not read to its end.
pub(super) struct Inbox {
    data_rx: mpsc::Receiver<Message>,
    control_rx: mpsc::UnboundedReceiver<WsError>,
    /// The end of the socket, held back until the frames that arrived before it have
    /// been handed over.
    end: Option<WsError>,
}

impl Inbox {
    fn new(data_rx: mpsc::Receiver<Message>, control_rx: mpsc::UnboundedReceiver<WsError>) -> Self {
        Self {
            data_rx,
            control_rx,
            end: None,
        }
    }

    /// Resolves with what ended the socket. For a socket nothing is read from yet —
    /// one that has not opened; it never resolves while the socket is alive.
    async fn next_end(&mut self) -> WsError {
        match self.control_rx.next().await {
            Some(end) => end,
            // The senders live in the event handlers of the socket, so they are not
            // dropped while the socket is waited for.
            None => std::future::pending().await,
        }
    }
}

impl Stream for Inbox {
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `Inbox` is `Unpin` (all fields are), so the safe `Pin::get_mut` works and no
        // `unsafe` is needed.
        let this = Pin::get_mut(self);

        if this.end.is_none() {
            match Pin::new(&mut this.control_rx).poll_next(cx) {
                // Jumps the queue: see the type docs.
                Poll::Ready(Some(WsError::SlowConsumer)) => {
                    return Poll::Ready(Some(Err(WsError::SlowConsumer)));
                }
                // Held back until the frames that arrived before it are handed over.
                Poll::Ready(Some(end)) => this.end = Some(end),
                Poll::Ready(None) | Poll::Pending => {}
            }
        }

        match Pin::new(&mut this.data_rx).poll_next(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            // No frame is left: nothing stands in front of the end any more.
            Poll::Pending => match this.end.take() {
                Some(end) => Poll::Ready(Some(Err(end))),
                None => Poll::Pending,
            },
            Poll::Ready(None) => Poll::Ready(this.end.take().map(Err)),
        }
    }
}

pub(super) struct ManagedWs {
    ws: web_sys::WebSocket,
    /// Resolved by the `open` event of the socket. Taken by
    /// [`wait_for_open`](Self::wait_for_open).
    opened: Option<oneshot::Receiver<()>>,
    inbox: Inbox,
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
        let (opened_tx, opened_rx) = oneshot::channel::<()>();

        let mut opened_tx = Some(opened_tx);
        let on_open = Closure::wrap(Box::new(move || {
            if let Some(opened_tx) = opened_tx.take() {
                let _ = opened_tx.send(());
            }
        }) as Box<dyn FnMut()>);
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
            opened: Some(opened_rx),
            inbox: Inbox::new(data_rx, control_rx),
            _on_open: on_open,
            _on_message: on_message,
            _on_error: on_error,
            _on_close: on_close,
        })
    }

    /// Waits until the socket has opened, has failed to, or `timeout` has passed.
    ///
    /// The socket reports it itself — its `open` event resolves this — so nothing is
    /// polled, and a socket that opened is never missed: not one that was closed again
    /// right away (see [`connect_outcome`]), and not in a background tab, where the
    /// browser throttles timers to one a second or less.
    pub async fn wait_for_open(&mut self, timeout: Duration) -> Result<(), ConnectError> {
        let opened = self.opened.take();
        let opened = async move {
            let has_opened = match opened {
                Some(opened) => opened.await.is_ok(),
                None => false,
            };

            if !has_opened {
                // Asked a second time, or the `open` handler is gone without having
                // fired: this is not how the socket is going to turn out.
                std::future::pending::<()>().await;
            }
        };

        connect_outcome(
            opened,
            self.inbox.next_end(),
            dioxus_utils::js::sleep(timeout),
        )
        .await
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
        Pin::new(&mut Pin::get_mut(self).inbox).poll_next(cx)
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

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};

    use futures::executor::block_on;

    use super::*;

    fn closed(code: u16, reason: &str) -> WsError {
        WsError::Closed {
            code,
            reason: reason.to_string(),
        }
    }

    // ---------------------------------------------------------------- connect_outcome

    #[test]
    fn a_socket_that_opened_is_a_connection() {
        let outcome = block_on(connect_outcome(
            ready(()),
            pending::<WsError>(),
            pending::<()>(),
        ));

        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn a_socket_that_opened_and_was_closed_since_is_a_connection_all_the_same() {
        // The server answered with a few frames and a close, and all of it happened
        // before the client got to look. It still connected: its frames and its close
        // are what the read loop is for.
        let outcome = block_on(connect_outcome(
            ready(()),
            ready(closed(1000, "")),
            pending::<()>(),
        ));

        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn a_socket_that_opened_just_as_the_time_ran_out_is_a_connection() {
        let outcome = block_on(connect_outcome(ready(()), pending::<WsError>(), ready(())));

        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn a_socket_closed_before_it_opened_is_not_a_connection_and_says_why() {
        let outcome = block_on(connect_outcome(
            pending::<()>(),
            ready(closed(1006, "")),
            pending::<()>(),
        ));

        assert_eq!(outcome, Err(ConnectError::Failed(closed(1006, ""))));
    }

    #[test]
    fn a_failure_is_reported_as_one_even_when_the_time_ran_out_as_well() {
        let outcome = block_on(connect_outcome(
            pending::<()>(),
            ready(WsError::ConnectionError),
            ready(()),
        ));

        assert_eq!(outcome, Err(ConnectError::Failed(WsError::ConnectionError)));
    }

    #[test]
    fn a_socket_that_neither_opens_nor_fails_times_out() {
        let outcome = block_on(connect_outcome(
            pending::<()>(),
            pending::<WsError>(),
            ready(()),
        ));

        assert_eq!(outcome, Err(ConnectError::TimedOut));
    }

    #[test]
    fn a_socket_nothing_happened_to_yet_is_waited_for() {
        let outcome =
            connect_outcome(pending::<()>(), pending::<WsError>(), pending::<()>()).now_or_never();

        assert_eq!(outcome, None);
    }

    #[test]
    fn why_a_socket_did_not_connect_reads_as_a_log_line() {
        assert_eq!(ConnectError::TimedOut.to_string(), "connect timeout");
        assert_eq!(
            ConnectError::Failed(closed(1006, "")).to_string(),
            "connect failed: closed (code=1006, reason=\"\")"
        );
    }

    // -------------------------------------------------------------------------- Inbox

    struct Socket {
        data_tx: mpsc::Sender<Message>,
        control_tx: mpsc::UnboundedSender<WsError>,
    }

    impl Socket {
        fn frame(&mut self, text: &str) {
            self.data_tx
                .try_send(Message::Text(text.to_string()))
                .unwrap();
        }

        fn ends_with(&self, end: WsError) {
            self.control_tx.unbounded_send(end).unwrap();
        }
    }

    fn inbox() -> (Socket, Inbox) {
        let (data_tx, data_rx) = mpsc::channel(INCOMING_BUFFER_SIZE);
        let (control_tx, control_rx) = mpsc::unbounded();

        let socket = Socket {
            data_tx,
            control_tx,
        };

        (socket, Inbox::new(data_rx, control_rx))
    }

    /// What the inbox hands over right now, as text — `None` when it has nothing yet.
    fn next(inbox: &mut Inbox) -> Option<String> {
        let item = inbox.next().now_or_never()?;

        Some(match item {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Bytes(bytes))) => format!("{bytes:?}"),
            Some(Err(err)) => format!("end: {err}"),
            None => "the stream is over".to_string(),
        })
    }

    fn drained(inbox: &mut Inbox) -> Vec<String> {
        std::iter::from_fn(|| next(inbox)).collect()
    }

    #[test]
    fn frames_that_arrived_before_a_close_are_handed_over_before_it() {
        let (mut socket, mut inbox) = inbox();

        socket.frame("first");
        socket.frame("second");
        socket.frame("third");
        socket.ends_with(closed(1000, "done"));

        assert_eq!(
            drained(&mut inbox),
            [
                "first",
                "second",
                "third",
                "end: closed (code=1000, reason=\"done\")"
            ]
        );
    }

    #[test]
    fn frames_that_arrived_before_an_error_are_handed_over_before_it() {
        let (mut socket, mut inbox) = inbox();

        socket.frame("first");
        socket.ends_with(WsError::ConnectionError);

        assert_eq!(drained(&mut inbox), ["first", "end: connection error"]);
    }

    #[test]
    fn a_close_with_nothing_in_front_of_it_is_handed_over_at_once() {
        let (socket, mut inbox) = inbox();

        socket.ends_with(closed(4401, "invalid token"));

        assert_eq!(
            next(&mut inbox).as_deref(),
            Some("end: closed (code=4401, reason=\"invalid token\")")
        );
    }

    #[test]
    fn the_first_thing_that_ended_the_socket_is_what_it_ended_with() {
        // A failed socket fires `error` and then `close`.
        let (mut socket, mut inbox) = inbox();

        socket.frame("first");
        socket.ends_with(WsError::ConnectionError);
        socket.ends_with(closed(1006, ""));

        assert_eq!(drained(&mut inbox)[..2], ["first", "end: connection error"]);
    }

    #[test]
    fn a_slow_consumer_signal_is_handed_over_ahead_of_the_buffered_frames() {
        let (mut socket, mut inbox) = inbox();

        socket.frame("first");
        socket.frame("second");
        socket.ends_with(WsError::SlowConsumer);

        assert_eq!(
            next(&mut inbox).as_deref(),
            Some("end: slow consumer: inbound buffer overflow")
        );
    }

    #[test]
    fn frames_keep_their_order() {
        let (mut socket, mut inbox) = inbox();

        for no in 0..100 {
            socket.frame(&no.to_string());
        }

        let expected: Vec<String> = (0..100).map(|no| no.to_string()).collect();
        assert_eq!(drained(&mut inbox), expected);
    }

    #[test]
    fn a_socket_that_is_alive_and_silent_is_waited_for() {
        let (mut socket, mut inbox) = inbox();
        assert_eq!(next(&mut inbox), None);

        socket.frame("first");
        assert_eq!(next(&mut inbox).as_deref(), Some("first"));
        assert_eq!(next(&mut inbox), None);

        // ...and goes on once something comes
        socket.frame("second");
        assert_eq!(next(&mut inbox).as_deref(), Some("second"));
    }

    #[test]
    fn a_socket_that_has_not_ended_gives_no_end() {
        let (_socket, mut inbox) = inbox();

        assert_eq!(inbox.next_end().now_or_never(), None);
    }

    #[test]
    fn what_ended_a_socket_nothing_is_read_from_is_told_as_it_is() {
        let (socket, mut inbox) = inbox();

        socket.ends_with(closed(1006, ""));

        assert_eq!(inbox.next_end().now_or_never(), Some(closed(1006, "")));
    }
}
