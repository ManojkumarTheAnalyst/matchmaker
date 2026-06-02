use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
    Router,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::engine::{GameMatch, MatchmakingEngine};
use crate::metrics::MetricsSnapshot;
use crate::player::{JoinRequest, PlayerEntry};

pub type AppState = Arc<MatchmakingEngine>;

/// Wire all HTTP routes to their handler functions
pub fn create_router(engine: Arc<MatchmakingEngine>) -> Router {
    Router::new()
        .route("/player/join",      post(join_player))
        .route("/player/:id/leave", delete(leave_player))
        .route("/health",           get(health))
        .route("/metrics",          get(metrics_handler))
        .route("/matches/recent",   get(recent_matches))
        .route("/pool/size",        get(pool_size))
        .with_state(engine)
}

// ── POST /player/join ─────────────────────────────────────────────────────────
// Body: { "player_id": "abc123", "mmr": 1500.0, "region": "us-east" }
// Returns 200 if queued, 409 if already in queue, 400 if bad data

async fn join_player(
    State(engine): State<AppState>,
    Json(req): Json<JoinRequest>,
) -> Result<Json<Value>, StatusCode> {

    // Validate input
    if req.player_id.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if req.mmr < 0.0 || req.mmr > 10_000.0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let entry = PlayerEntry::new(
        req.player_id.clone(),
        req.mmr,
        req.region.clone(),
    );

    if engine.pool.join(entry) {
        tracing::debug!(
            player_id = %req.player_id,
            mmr = req.mmr,
            "Player joined queue"
        );
        Ok(Json(json!({
            "status":    "queued",
            "player_id": req.player_id,
            "mmr":       req.mmr,
            "region":    req.region,
        })))
    } else {
        // Player is already in the queue
        Err(StatusCode::CONFLICT)
    }
}

// ── DELETE /player/:id/leave ──────────────────────────────────────────────────
// Returns 200 if removed, 404 if player not found

async fn leave_player(
    State(engine): State<AppState>,
    Path(id): Path<String>,
) -> StatusCode {
    if engine.pool.leave(&id) {
        tracing::debug!(player_id = %id, "Player left queue");
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// ── GET /health ───────────────────────────────────────────────────────────────
// Fast health check — used by load balancers and monitoring tools

async fn health(State(engine): State<AppState>) -> Json<Value> {
    let snap = engine.metrics.snapshot();
    Json(json!({
        "status":         "ok",
        "pool_size":      engine.pool.len(),
        "matches_formed": snap.matches_formed,
        "avg_quality":    snap.avg_quality,
        "avg_wait_ms":    snap.avg_wait_ms,
    }))
}

// ── GET /metrics ──────────────────────────────────────────────────────────────
// Full performance metrics snapshot

async fn metrics_handler(
    State(engine): State<AppState>,
) -> Json<MetricsSnapshot> {
    Json(engine.metrics.snapshot())
}

// ── GET /matches/recent ───────────────────────────────────────────────────────
// Returns last 200 completed matches with full team details

async fn recent_matches(
    State(engine): State<AppState>,
) -> Json<Vec<GameMatch>> {
    let matches = engine.recent_matches.read();
    Json(matches.clone())
}

// ── GET /pool/size ────────────────────────────────────────────────────────────
// Quick check of how many players are currently waiting

async fn pool_size(State(engine): State<AppState>) -> Json<Value> {
    Json(json!({
        "pool_size": engine.pool.len()
    }))
}