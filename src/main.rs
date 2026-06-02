mod api;
mod config;
mod engine;
mod metrics;
mod player;
mod pool;

use std::sync::Arc;
use config::Config;
use engine::MatchmakingEngine;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Set up logging
    // Run with RUST_LOG=matchmaker=debug for detailed logs
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("matchmaker=info")),
        )
        .init();

    let config = Config::default();
    let port   = config.port;

    // Build the engine
    let engine = Arc::new(MatchmakingEngine::new(config));

    // Start background matching workers BEFORE accepting HTTP traffic
    engine.start_workers();

    // Build HTTP router
    let app = api::create_router(Arc::clone(&engine));

    // Bind to port
    let addr     = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", addr, e));

    tracing::info!(
        "Matchmaker running on http://localhost:{}",
        port
    );
    tracing::info!(
        "Workers: {} | Base MMR range: ±{}",
        engine.config.num_workers,
        engine.config.base_mmr_range
    );

    // Start serving requests
    axum::serve(listener, app)
        .await
        .expect("Server crashed");
}