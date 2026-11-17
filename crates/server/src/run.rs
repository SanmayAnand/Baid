//! Serving both listeners and shutting down cleanly.

use std::future::IntoFuture;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::http::{admin_router, public_router};
use crate::state::AppState;

/// Serves the public and admin APIs until `shutdown` is cancelled, then stops accepting
/// connections and waits for in-flight requests to finish.
pub async fn serve(
    state: AppState,
    public: TcpListener,
    admin: TcpListener,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let public_srv = axum::serve(public, public_router(state.clone()))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let admin_srv = axum::serve(admin, admin_router(state.clone()))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let chores = async {
        housekeeping(state, shutdown).await;
        Ok(())
    };
    tokio::try_join!(public_srv, admin_srv, chores)?;
    Ok(())
}

/// Sessions idle this long are forgotten.
pub const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);
/// How often housekeeping runs.
pub const HOUSEKEEPING_EVERY: Duration = Duration::from_secs(60);

/// Expires idle sessions and old idempotency keys every minute until `shutdown`.
pub async fn housekeeping(state: AppState, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(HOUSEKEEPING_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {
                let (sessions, keys) = state.engine.housekeeping(Instant::now(), SESSION_IDLE);
                if sessions + keys > 0 {
                    debug!(sessions, idempotency_keys = keys, "housekeeping");
                }
            }
        }
    }
}

/// Waits for Ctrl-C (or SIGTERM on Unix), then marks the service not ready and cancels
/// `shutdown`.
pub async fn shutdown_on_signal(state: AppState, shutdown: CancellationToken) {
    wait_for_signal().await;
    info!("shutdown requested; draining");
    state.set_ready(false);
    shutdown.cancel();
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
