//! HTTP API for blackboard inspection

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use crate::blackboard::Blackboard;
use crate::websocket::websocket_handler;
use robot_bt_protocol::{
    BlackboardKeysResponse,
    BlackboardKeyInfo,
    BlackboardKeyValueResponse,
    ErrorResponse,
};

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub blackboard: Blackboard,
}

/// Create the HTTP API router with WebSocket support
pub fn create_api_router(blackboard: Blackboard) -> Router {
    let state = AppState { blackboard };

    Router::new()
        .route("/api/blackboard", get(get_all_keys))
        .route("/api/blackboard/{key}", get(get_key_value))
        .route("/api/ws", get(websocket_handler))
        .with_state(state)
}

/// GET /api/blackboard — returns all keys metadata
async fn get_all_keys(
    State(state): State<AppState>,
) -> Result<Json<BlackboardKeysResponse>, (StatusCode, Json<ErrorResponse>)> {
    let metadata = state.blackboard.get_keys_metadata().await;

    let keys = metadata
        .into_iter()
        .map(|meta| BlackboardKeyInfo {
            key: meta.key,
            type_name: meta.type_name,
        })
        .collect();

    Ok(Json(BlackboardKeysResponse { keys }))
}

/// GET /api/blackboard/{key} — returns specific key value (JSON serialized on demand)
async fn get_key_value(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<BlackboardKeyValueResponse>, (StatusCode, Json<ErrorResponse>)> {
    if !state.blackboard.contains(&key).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: format!("Key '{}' not found", key) }),
        ));
    }

    match state.blackboard.get_json_value_with_type(&key).await {
        Some((value, type_name)) => Ok(Json(BlackboardKeyValueResponse { key, type_name, value })),
        None => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to serialize value for key '{}'", key) }),
        )),
    }
}
