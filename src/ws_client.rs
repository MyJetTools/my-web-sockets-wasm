use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::spawn;
use futures::StreamExt;

use super::managed_ws::ManagedWs;
use super::{WsCallback, WsConnection, WsState};

struct WebSocketClientInner {
    reconnect_timeout: Duration,
    /// How long a socket must stay `OPEN` for the connection to count as a real,
    /// working one rather than a flapping attempt. When such a connection drops,
    /// the next attempt skips [`reconnect_timeout`](WebSocketClientInner::reconnect_timeout)
    /// and reconnects at once.
    stable_connection_threshold: Duration,
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
    /// Create a client with the default timeouts: reconnect 3s (skipped once after
    /// a connection that stayed open longer than 20s), connect 10s, init 13s,
    /// read 10s.
    pub fn new() -> Self {
        Self {
            inner: Rc::new(WebSocketClientInner {
                reconnect_timeout: Duration::from_secs(3),
                stable_connection_threshold: Duration::from_secs(20),
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

/// Whether this connection attempt should skip [`WebSocketClientInner::reconnect_timeout`].
///
/// `previous_connected_ms` is the [`now_ms`] stamp of the moment the PREVIOUS socket
/// reached `OPEN`, or `None` if none ever did or the credit it carries was already spent.
/// A connection that stayed open longer than `stable_threshold_ms` was a real, working
/// connection that merely dropped — we want it back as soon as possible. Anything shorter
/// is flapping (auth reject, init timeout, instant server close) and has to go through the
/// backoff, or a rejecting server gets hammered.
///
/// Two properties keep this from ever turning into a hammer:
/// - the credit is **one-shot** — every attempt spends it, so a server that stays down is
///   retried on the plain backoff instead of in a tight loop;
/// - a fresh credit can only be minted by a socket that actually reached `OPEN`, and only
///   pays out `stable_threshold_ms` later, so two immediate reconnects are always at least
///   that far apart.
fn should_skip_backoff(
    previous_connected_ms: Option<f64>,
    now_ms: f64,
    stable_threshold_ms: f64,
) -> bool {
    match previous_connected_ms {
        Some(connected_ms) => now_ms - connected_ms > stable_threshold_ms,
        None => false,
    }
}

async fn connection_loop<TCallback: WsCallback>(
    inner: Rc<WebSocketClientInner>,
    callback: Rc<TCallback>,
) {
    let mut first_iteration = true;
    // `now_ms()` stamp of the moment the PREVIOUS socket reached `OPEN`, or `None`
    // if none ever did or the fast-reconnect credit it carries was already spent.
    // Every iteration of this loop is exactly one connection attempt, and every
    // attempt spends the credit — see `should_skip_backoff`.
    let mut last_connected_ms: Option<f64> = None;

    while inner.is_working() {
        // Decided before `get_url()`, so the URL is always fetched immediately
        // before the attempt it belongs to (callers refresh tokens inside it).
        let skip_backoff = should_skip_backoff(
            last_connected_ms,
            now_ms(),
            inner.stable_connection_threshold.as_millis() as f64,
        );

        // `skip_backoff` is never true on the first iteration — there is no credit
        // yet — so the first attempt after `start()` is immediate either way.
        if !first_iteration && !skip_backoff {
            dioxus_utils::js::sleep(inner.reconnect_timeout).await;
        }
        first_iteration = false;

        // No URL yet (e.g. not logged in). Idle here rather than burning a loop
        // iteration on it: a momentary `None` — a token being refreshed at the
        // instant the socket dropped — must not cost a stable connection its
        // immediate retry. `get_url` is re-polled after every wait.
        let url = loop {
            if let Some(url) = callback.get_url() {
                break url;
            }
            dioxus_utils::js::sleep(inner.reconnect_timeout).await;
            if !inner.is_working() {
                return;
            }
        };

        // We are really attempting now, so spend the credit whatever the outcome:
        // a server that stays down has to fall back to the plain backoff instead of
        // being retried in a tight loop.
        last_connected_ms = None;

        if skip_backoff {
            dioxus_utils::console_log("WS: previous connection was stable, reconnecting now");
        }
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

        // The socket is OPEN: mint the fast-reconnect credit. If this connection
        // lasts longer than `stable_connection_threshold`, the next attempt skips
        // the backoff.
        last_connected_ms = Some(now_ms());

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

#[cfg(test)]
mod tests {
    use super::{WebSocketClient, should_skip_backoff};
    use std::time::Duration;

    /// The default `stable_connection_threshold`, in milliseconds.
    const STABLE_MS: f64 = 20_000.0;

    #[test]
    fn defaults_keep_the_flapping_paths_throttled() {
        let inner = WebSocketClient::new().inner;
        assert_eq!(
            inner.stable_connection_threshold,
            Duration::from_secs(STABLE_MS as u64 / 1_000),
        );
        // A socket torn down by `init_timeout` — the caller forgot
        // `mark_initialized()` — must measure under the threshold, or that cycle
        // would earn itself a fast retry every single round.
        assert!(inner.init_timeout < inner.stable_connection_threshold);
        // Likewise a socket that opened and then went silent straight away.
        assert!(inner.read_timeout < inner.stable_connection_threshold);
    }

    #[test]
    fn no_previous_connection_keeps_the_backoff() {
        // Nothing ever reached OPEN — connect timeout, open() error, no URL.
        assert!(!should_skip_backoff(None, 5_000.0, STABLE_MS));
    }

    #[test]
    fn short_lived_connection_keeps_the_backoff() {
        // Opened at 1s, gone by 6s: 5s alive is flapping, not a real session.
        assert!(!should_skip_backoff(Some(1_000.0), 6_000.0, STABLE_MS));
    }

    #[test]
    fn init_timeout_cycle_keeps_the_backoff() {
        // A caller that forgets `mark_initialized()` drops at ~13s, under the 20s
        // threshold, so it stays throttled instead of spinning.
        assert!(!should_skip_backoff(Some(1_000.0), 14_000.0, STABLE_MS));
    }

    #[test]
    fn connection_alive_exactly_the_threshold_keeps_the_backoff() {
        // Strictly greater than, per the spec: 20s exactly is not yet stable.
        assert!(!should_skip_backoff(Some(1_000.0), 21_000.0, STABLE_MS));
    }

    #[test]
    fn connection_alive_past_the_threshold_skips_the_backoff() {
        assert!(should_skip_backoff(Some(1_000.0), 21_000.1, STABLE_MS));
    }

    #[test]
    fn long_lived_connection_skips_the_backoff() {
        // An hour-long session that just dropped: reconnect immediately.
        assert!(should_skip_backoff(Some(1_000.0), 3_601_000.0, STABLE_MS));
    }

    #[test]
    fn unavailable_performance_clock_keeps_the_backoff() {
        // `now_ms()` degrades to 0.0 when `performance` is missing, so every
        // measurement is 0 - 0 = 0 and we fall back to the plain backoff.
        assert!(!should_skip_backoff(Some(0.0), 0.0, STABLE_MS));
    }
}
