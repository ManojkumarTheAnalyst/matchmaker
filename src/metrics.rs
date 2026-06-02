use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use serde::Serialize;

// ─────────────────────────────────────────────────────────────────────────────
// Metrics — all lock-free using atomic operations only
//
// Why atomics instead of Mutex?
//   A Mutex forces threads to wait in line to update a counter.
//   Atomics update in a single CPU instruction — no waiting, ever.
//
// Floating-point averages are stored as scaled integers:
//   avg_wait stored as milliseconds × 1000  (3 decimal places)
//   avg_quality stored as score × 10000     (4 decimal places)
//   avg_cycle stored as microseconds × 100  (2 decimal places)
//
// We use Exponential Moving Average (EMA) for smooth live stats:
//   new_avg = (old_avg × 0.9) + (new_value × 0.1)
//   This gives more weight to recent data without storing history.
// ─────────────────────────────────────────────────────────────────────────────

pub struct Metrics {
    /// Total matches successfully formed
    pub matches_formed: AtomicU64,

    /// Total individual players matched (always matches_formed × 10)
    pub players_matched: AtomicU64,

    /// Total matching cycles run across all workers
    pub cycle_count: AtomicU64,

    /// EMA of player wait time — stored as (ms × 1000) for precision
    ema_wait_ms_x1000: AtomicI64,

    /// EMA of match quality score — stored as (score × 10000)
    /// Starts at 10000 which represents 1.0 (perfect quality)
    ema_quality_x10000: AtomicI64,

    /// EMA of how long each matching cycle takes — stored as (μs × 100)
    ema_cycle_us_x100: AtomicI64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            matches_formed:     AtomicU64::new(0),
            players_matched:    AtomicU64::new(0),
            cycle_count:        AtomicU64::new(0),
            ema_wait_ms_x1000:  AtomicI64::new(0),
            ema_quality_x10000: AtomicI64::new(10_000), // start at 1.0
            ema_cycle_us_x100:  AtomicI64::new(0),
        }
    }

    /// Called every time a match is successfully formed.
    /// Updates match count + EMA averages for wait time and quality.
    pub fn record_match(&self, quality: f64, avg_wait_secs: f64) {
        self.matches_formed.fetch_add(1, Ordering::Relaxed);
        self.players_matched.fetch_add(10, Ordering::Relaxed);

        // Update EMA for wait time (alpha = 0.1)
        let new_wait = (avg_wait_secs * 1_000.0 * 1_000.0) as i64;
        let cur_wait = self.ema_wait_ms_x1000.load(Ordering::Relaxed);
        self.ema_wait_ms_x1000.store(
            (cur_wait as f64 * 0.9 + new_wait as f64 * 0.1) as i64,
            Ordering::Relaxed,
        );

        // Update EMA for quality score (alpha = 0.1)
        let new_q = (quality * 10_000.0) as i64;
        let cur_q = self.ema_quality_x10000.load(Ordering::Relaxed);
        self.ema_quality_x10000.store(
            (cur_q as f64 * 0.9 + new_q as f64 * 0.1) as i64,
            Ordering::Relaxed,
        );
    }

    /// Called after every matching cycle with how long it took.
    pub fn record_cycle(&self, duration_micros: u64) {
        self.cycle_count.fetch_add(1, Ordering::Relaxed);

        // Update EMA for cycle time (alpha = 0.05 — extra smooth)
        let new_c = (duration_micros * 100) as i64;
        let cur_c = self.ema_cycle_us_x100.load(Ordering::Relaxed);
        self.ema_cycle_us_x100.store(
            (cur_c as f64 * 0.95 + new_c as f64 * 0.05) as i64,
            Ordering::Relaxed,
        );
    }

    /// Produce a snapshot of all current metrics.
    /// Completely non-blocking — just reads atomic values.
    /// Safe to call from HTTP health-check handlers at any time.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            matches_formed:  self.matches_formed.load(Ordering::Relaxed),
            players_matched: self.players_matched.load(Ordering::Relaxed),
            cycle_count:     self.cycle_count.load(Ordering::Relaxed),
            avg_wait_ms:     self.ema_wait_ms_x1000.load(Ordering::Relaxed)
                                 as f64 / 1_000.0,
            avg_quality:     self.ema_quality_x10000.load(Ordering::Relaxed)
                                 as f64 / 10_000.0,
            avg_cycle_us:    self.ema_cycle_us_x100.load(Ordering::Relaxed)
                                 as f64 / 100.0,
        }
    }
}

// ─── What gets sent back when someone calls GET /metrics ────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// Total matches formed since server started
    pub matches_formed: u64,

    /// Total players matched (matches_formed × 10)
    pub players_matched: u64,

    /// Total matching cycles run
    pub cycle_count: u64,

    /// Average player wait time in milliseconds (EMA)
    pub avg_wait_ms: f64,

    /// Average match quality 0.0 to 1.0 (EMA) — 1.0 is perfectly balanced
    pub avg_quality: f64,

    /// Average time per matching cycle in microseconds (EMA)
    pub avg_cycle_us: f64,
}