//! WebSocket server for real-time blackboard updates

use axum::extract::{
    ws::{Message, WebSocket, WebSocketUpgrade},
    State,
};
use axum::response::Response;
use futures::{sink::SinkExt, stream::StreamExt};
use robot_bt_protocol::{WsClientMessage, WsServerMessage};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::blackboard::{Blackboard, BlackboardValue};
use crate::http_api::AppState;

/// WebSocket connection handler
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, state.blackboard))
}

/// Handle individual WebSocket connection
async fn handle_websocket(socket: WebSocket, blackboard: Blackboard) {
    info!("New WebSocket connection established");

    let (mut sender, mut receiver) = socket.split();

    // Current set of per-key watch receivers for this connection.
    // Rebuilt whenever the client sends a Watch message.
    let mut watch_rxs: Vec<(String, watch::Receiver<Option<Arc<dyn BlackboardValue>>>)> = Vec::new();

    loop {
        // Build a select over all active watch receivers plus incoming client messages.
        // We poll receivers by index so we know which key fired.
        tokio::select! {
            // Incoming client message
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        info!("Received WebSocket message: {}", text);
                        match handle_client_message(&text, &blackboard, &mut sender).await {
                            Ok(Some(new_rxs)) => watch_rxs = new_rxs,
                            Ok(None) => {}
                            Err(e) => error!("Error handling client message: {}", e),
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("WebSocket connection closed");
                        break;
                    }
                    Some(Err(e)) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }

            // Any watch channel fired — find and forward all changed ones
            _ = wait_any_changed(&mut watch_rxs) => {
                for (key, rx) in &mut watch_rxs {
                    if !rx.has_changed().unwrap_or(false) {
                        continue;
                    }
                    let val = rx.borrow_and_update();
                    if let Some(ref arc) = *val {
                        if let Some(json) = arc.to_json() {
                            let type_name = arc.type_name().to_string();
                            let msg = WsServerMessage::Update {
                                key: key.clone(),
                                type_name,
                                value: json,
                            };
                            if let Ok(text) = serde_json::to_string(&msg) {
                                if sender.send(Message::text(text)).await.is_err() {
                                    debug!("Failed to send update, client disconnected");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Returns when at least one receiver has a pending change.
async fn wait_any_changed(rxs: &mut [(String, watch::Receiver<Option<Arc<dyn BlackboardValue>>>)]) {
    if rxs.is_empty() {
        // Nothing to watch — park forever (outer select will still handle messages).
        std::future::pending::<()>().await;
        return;
    }
    // Poll each receiver; the first one to fire wins.
    let futs = rxs.iter_mut().map(|(_, rx)| {
        Box::pin(async move { rx.changed().await })
    });
    futures::future::select_all(futs).await;
}

/// Handle an incoming client message.
///
/// Returns `Ok(Some(receivers))` when the watch set should be replaced,
/// `Ok(None)` when no change to the watch set is needed.
async fn handle_client_message(
    text: &str,
    blackboard: &Blackboard,
    sender: &mut (impl SinkExt<Message, Error = axum::Error> + Unpin),
) -> Result<
    Option<Vec<(String, watch::Receiver<Option<Arc<dyn BlackboardValue>>>)>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let message: WsClientMessage = serde_json::from_str(text)?;

    match message {
        WsClientMessage::Watch { keys } => {
            info!("Now watching {} keys: {:?}", keys.len(), keys);

            let mut rxs = Vec::with_capacity(keys.len());
            for key in &keys {
                let rx = blackboard.watch(key).await;

                // Send the current value immediately if one exists.
                if let Some(ref arc) = *rx.borrow() {
                    if let Some(json) = arc.to_json() {
                        let type_name = arc.type_name().to_string();
                        let msg = WsServerMessage::Update {
                            key: key.clone(),
                            type_name,
                            value: json,
                        };
                        if let Ok(text) = serde_json::to_string(&msg) {
                            let _ = sender.send(Message::text(text)).await;
                        }
                    }
                }

                rxs.push((key.clone(), rx));
            }

            Ok(Some(rxs))
        }
    }
}
