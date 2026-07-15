use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::spawn;
use futures::StreamExt;

use super::managed_ws::ManagedWs;
use super::{WsCallback, WsConnection, WsState};

struct WebSocketClientInner {
    reconnect_timeout: Duration,
    connect_timeout: Duration,
    init_timeout: Duration,
    read_timeout: Duration,
    working: Cell<bool>,
}

impl WebSocketClientInner {
    fn is_working(&self) -> bool {
        self.working.get()
    }
}

/// Auto-reconnecting browser (WASM) WebSocket client. See the crate docs for the
/// connection lifecycle and the mandatory [`WsConnection::mark_initialized`]
/// contract.
pub struct WebSocketClient {
    inner: Rc<WebSocketClientInner>,
}

impl WebSocketClient {
    /// Create a client with the default timeouts: reconnect 3s, connect 10s,
    /// init 13s, read 10s.
    pub fn new() -> Self {
        Self {
            inner: Rc::new(WebSocketClientInner {
                reconnect_timeout: Duration::from_secs(3),
                connect_timeout: Duration::from_secs(10),
                init_timeout: Duration::from_secs(13),
                read_timeout: Duration::from_secs(10),
                working: Cell::new(true),
            }),
        }
    }

    /// Spawn the reconnect loop, driving the given callback. **Must be called from
    /// within a Dioxus reactive context** (it uses `dioxus::prelude::spawn`).
    /// Intended to be called once per client; calling it again after
    /// [`stop`](Self::stop) resumes the loop.
    pub fn start<TCallback: WsCallback>(&self, callback: Rc<TCallback>) {
        self.inner.working.set(true);
        let inner = self.inner.clone();
        spawn(connection_loop(inner, callback));
    }

    /// Stop the reconnect loop. The current connection is torn down at the next
    /// loop iteration. Call [`start`](Self::start) again to resume.
    pub fn stop(&self) {
        self.inner.working.set(false);
    }
}

impl Default for WebSocketClient {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// The browser `Performance` object — a monotonic high-resolution clock.
    /// Cached once (WASM is single-threaded) so we don't re-do a
    /// `window().performance()` lookup on every read-loop tick.
    static PERFORMANCE: Option<web_sys::Performance> =
        web_sys::window().and_then(|w| w.performance());
}

/// Monotonic millisecond clock via `performance.now()`, unaffected by wall-clock
/// changes (NTP sync, manual clock steps, sleep/resume) — the correct source for
/// measuring timeouts. Falls back to `0.0` only if `performance` is unavailable.
fn now_ms() -> f64 {
    PERFORMANCE.with(|p| p.as_ref().map(|p| p.now()).unwrap_or(0.0))
}

async fn connection_loop<TCallback: WsCallback>(
    inner: Rc<WebSocketClientInner>,
    callback: Rc<TCallback>,
) {
    let mut first_iteration = true;
    while inner.is_working() {
        if !first_iteration {
            dioxus_utils::js::sleep(inner.reconnect_timeout).await;
        }
        first_iteration = false;

        let Some(url) = callback.get_url() else {
            // No URL yet (e.g. not logged in). The loop-top sleep provides the
            // retry backoff on subsequent iterations, so we just continue.
            continue;
        };

        dioxus_utils::console_log(format!("Connecting to WS {}", url));

        let mut managed = match ManagedWs::open(&url) {
            Ok(ws) => ws,
            Err(err) => {
                dioxus_utils::console_log(format!("Cannot open WS: {:?}", err));
                continue;
            }
        };

        let conn = Rc::new(WsConnection::new(managed.ws_handle()));

        if !wait_for_open(&managed, inner.connect_timeout).await {
            dioxus_utils::console_log("WS: connect timeout");
            conn.disconnect();
            continue;
        }

        if callback.on_connected(conn.clone()).await.is_err() {
            conn.disconnect();
            callback.on_disconnected(conn).await;
            continue;
        }

        run_read_loop(&mut managed, &inner, conn.clone(), callback.clone()).await;

        callback.on_disconnected(conn).await;
        // managed dropped here → ManagedWs::Drop detaches all listeners and closes WS.
    }
}

async fn wait_for_open(managed: &ManagedWs, timeout: Duration) -> bool {
    let poll = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        match managed.state() {
            WsState::Open => return true,
            WsState::Closing | WsState::Closed => return false,
            WsState::Connecting => {}
        }
        if elapsed >= timeout {
            return false;
        }
        dioxus_utils::js::sleep(poll).await;
        elapsed += poll;
    }
}

async fn run_read_loop<TCallback: WsCallback>(
    managed: &mut ManagedWs,
    inner: &Rc<WebSocketClientInner>,
    conn: Rc<WsConnection>,
    callback: Rc<TCallback>,
) {
    use futures::FutureExt;

    let started_ms = now_ms();
    let mut last_msg_ms = started_ms;

    while conn.is_connected() && inner.is_working() {
        let now = now_ms();
        let elapsed_ms = (now - started_ms) as u64;

        if !conn.is_initialized() && elapsed_ms >= inner.init_timeout.as_millis() as u64 {
            dioxus_utils::console_log("WS: init timeout");
            conn.disconnect();
            break;
        }
        if conn.is_initialized() {
            let since_msg_ms = (now - last_msg_ms) as u64;
            if since_msg_ms >= inner.read_timeout.as_millis() as u64 {
                dioxus_utils::console_log("WS: read timeout");
                conn.disconnect();
                break;
            }
        }

        let next_wake_ms = if conn.is_initialized() {
            inner
                .read_timeout
                .as_millis()
                .saturating_sub((now - last_msg_ms) as u128) as u64
        } else {
            inner
                .init_timeout
                .as_millis()
                .saturating_sub(elapsed_ms as u128) as u64
        }
        .max(50);

        let timer = dioxus_utils::js::sleep(Duration::from_millis(next_wake_ms)).fuse();
        let next_msg = managed.next().fuse();
        futures::pin_mut!(timer, next_msg);

        futures::select! {
            msg = next_msg => {
                let Some(msg) = msg else {
                    conn.disconnect();
                    break;
                };
                match msg {
                    Ok(payload) => {
                        if callback.on_data(conn.clone(), payload).await.is_err() {
                            conn.disconnect();
                            break;
                        }
                        // Stamp liveness AFTER processing so a slow on_data is not
                        // mistaken for server silence by the read-timeout.
                        last_msg_ms = now_ms();
                    }
                    Err(err) => {
                        dioxus_utils::console_log(format!("WS disconnected: {err}"));
                        conn.disconnect();
                        break;
                    }
                }
            }
            _ = timer => {
                // wakeup; loop top re-evaluates timeouts
            }
        }
    }
}
