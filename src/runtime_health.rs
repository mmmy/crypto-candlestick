use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
pub const HEALTH_STALE_MESSAGE_MS: i64 = 60_000;

#[derive(Debug, Clone, Default)]
pub struct RuntimeHealth {
    inner: Arc<RwLock<RuntimeHealthInner>>,
}

#[derive(Debug, Clone, Default)]
struct RuntimeHealthInner {
    websocket: WebSocketHealth,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketHealth {
    pub connected: bool,
    pub last_message_at: Option<String>,
    pub last_message_ago_ms: Option<i64>,
    pub reconnect_count: u64,
    pub last_error: Option<String>,
    pub ok: bool,
    pub reason: Option<&'static str>,
    #[serde(skip)]
    enabled: bool,
    #[serde(skip)]
    last_message_at_ms: Option<i64>,
}

impl Default for WebSocketHealth {
    fn default() -> Self {
        Self {
            connected: false,
            last_message_at: None,
            last_message_ago_ms: None,
            reconnect_count: 0,
            last_error: None,
            ok: true,
            reason: None,
            enabled: false,
            last_message_at_ms: None,
        }
    }
}

impl RuntimeHealth {
    pub async fn mark_connected(&self) {
        let mut inner = self.inner.write().await;
        let ws = &mut inner.websocket;
        ws.enabled = true;
        ws.connected = true;
        ws.last_error = None;
    }

    pub async fn mark_disconnected(&self, error: impl Into<String>) {
        let mut inner = self.inner.write().await;
        let ws = &mut inner.websocket;
        ws.enabled = true;
        ws.connected = false;
        ws.last_error = Some(error.into());
    }

    pub async fn mark_reconnecting(&self, error: impl Into<String>) {
        let mut inner = self.inner.write().await;
        let ws = &mut inner.websocket;
        ws.enabled = true;
        ws.connected = false;
        ws.reconnect_count += 1;
        ws.last_error = Some(error.into());
    }

    pub async fn mark_message_now(&self) {
        self.mark_message_at(chrono::Utc::now().timestamp_millis())
            .await;
    }

    pub async fn mark_message_at(&self, timestamp_ms: i64) {
        let mut inner = self.inner.write().await;
        let ws = &mut inner.websocket;
        ws.enabled = true;
        ws.last_message_at_ms = Some(timestamp_ms);
        ws.last_message_at = Some(crate::time_format::format_timestamp_ms(timestamp_ms));
        ws.last_error = None;
    }

    pub async fn websocket_snapshot(&self) -> WebSocketHealth {
        let inner = self.inner.read().await;
        let mut websocket = inner.websocket.clone();
        let now_ms = chrono::Utc::now().timestamp_millis();
        websocket.last_message_ago_ms = websocket
            .last_message_at_ms
            .map(|timestamp_ms| now_ms.saturating_sub(timestamp_ms));
        let stale = websocket
            .last_message_ago_ms
            .map(|age_ms| age_ms > HEALTH_STALE_MESSAGE_MS)
            .unwrap_or(false);
        websocket.ok = !websocket.enabled || (websocket.connected && !stale);
        websocket.reason = if !websocket.enabled || websocket.ok {
            None
        } else if !websocket.connected {
            Some("websocket is disconnected")
        } else {
            Some("websocket message stream is stale")
        };
        websocket
    }
}
