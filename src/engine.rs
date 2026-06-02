use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};
use std::time::Instant;
use parking_lot::RwLock;
use serde::Serialize;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::player::{MatchedPlayer, PlayerEntry};
use crate::pool::PlayerPool;

// ─── Match ID Generator ──────────────────────────────────────────────────────
// Generates unique IDs like: match-0001a2b3c4d5e6f7-00000001
// Uses timestamp + counter so IDs are always unique and sortable

static MATCH_SEQ: AtomicU64 = AtomicU64::new(1);

fn new_match_id() -> String {
    let seq = MATCH_SEQ.fetch_add(1, AtomOrd::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!("match-{:016x}-{:08x}", ts, seq)
}

// ─── Match Result (sent back via HTTP) ───────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct GameMatch {
    pub match_id:       String,
    pub team_a:         Vec<MatchedPlayer>,
    pub team_b:         Vec<MatchedPlayer>,
    pub team_a_avg_mmr: f64,
    pub team_b_avg_mmr: f64,
    /// Difference between highest and lowest MMR in the match
    pub mmr_spread:     f64,
    /// 1.0 = perfectly balanced teams, 0.0 = completely unbalanced
    pub quality_score:  f64,
    pub timestamp_ms:   u64,
}

// ─── The Engine ──────────────────────────────────────────────────────────────

pub struct MatchmakingEngine {
    pub pool:           Arc<PlayerPool>,
    pub metrics:        Arc<Metrics>,
    pub config:         Config,
    /// Last 200 matches stored in memory for the /matches endpoint
    pub recent_matches: Arc<RwLock<Vec<GameMatch>>>,
}

impl MatchmakingEngine {
    pub fn new(config: Config) -> Self {
        Self {
            pool:           Arc::new(PlayerPool::new()),
            metrics:        Arc::new(Metrics::new()),
            config,
            recent_matches: Arc::new(RwLock::new(Vec::with_capacity(200))),
        }
    }

    /// Start N worker tasks. Each runs forever in the background.
    pub fn start_workers(self: &Arc<Self>) {
        for worker_id in 0..self.config.num_workers {
            let engine = Arc::clone(self);
            tokio::spawn(async move {
                engine.worker_loop(worker_id).await;
            });
        }
        tracing::info!(
            "Started {} matching workers",
            self.config.num_workers
        );
    }

    // ── Worker Loop ──────────────────────────────────────────────────────────
    // Each worker runs this forever.
    // Matching logic is CPU-bound, so we use spawn_blocking to avoid
    // blocking the async executor (which handles HTTP requests).

    async fn worker_loop(&self, worker_id: usize) {
        let idle = std::time::Duration::from_millis(self.config.idle_sleep_ms);

        loop {
            let t0 = Instant::now();

            // Clone Arcs (cheap — just increments reference counters)
            let pool    = Arc::clone(&self.pool);
            let metrics = Arc::clone(&self.metrics);
            let recent  = Arc::clone(&self.recent_matches);
            let cfg     = self.config.clone();

            // Run CPU-heavy matching on a blocking thread
            let matches_formed = tokio::task::spawn_blocking(move || {
                run_matching_cycle(&pool, &metrics, &recent, &cfg, worker_id)
            })
            .await
            .unwrap_or(0);

            // Record how long this cycle took
            self.metrics.record_cycle(t0.elapsed().as_micros() as u64);

            if matches_formed == 0 {
                // Pool too small — sleep before trying again
                tokio::time::sleep(idle).await;
            } else {
                // Matched some players — yield and immediately try again
                tokio::task::yield_now().await;
            }
        }
    }
}

// ─── Core Matching Cycle ─────────────────────────────────────────────────────
//
// This function runs on a blocking thread (not async).
// It tries to form as many matches as possible in one pass.
//
// Time Complexity: O(n log n + n × k_avg)
//   n     = number of waiting players
//   k_avg = average players within one player's MMR window
//
// Space Complexity: O(n) for the snapshot + sorted index

fn run_matching_cycle(
    pool:    &PlayerPool,
    metrics: &Metrics,
    recent:  &RwLock<Vec<GameMatch>>,
    cfg:     &Config,
    _worker_id: usize,
) -> usize {

    // ── Step 1: Snapshot all WAITING players ─────────────────────────────
    let mut by_mmr = pool.snapshot_waiting();

    // Need at least 10 players to form one match
    if by_mmr.len() < 10 {
        return 0;
    }

    // ── Step 2: Sort by MMR for binary search range queries ─────────────
    // O(n log n)
    by_mmr.sort_unstable_by(|a, b| {
        a.mmr
            .partial_cmp(&b.mmr)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let n = by_mmr.len();

    // ── Step 3: Priority order — longest waiting player goes first ───────
    // This ensures no player starves (waits forever without a match)
    let mut priority: Vec<usize> = (0..n).collect();
    priority.sort_unstable_by(|&a, &b| {
        by_mmr[b]
            .wait_secs()
            .partial_cmp(&by_mmr[a].wait_secs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Track which players in our snapshot are already used
    let mut used   = vec![false; n];
    let mut formed = 0usize;

    // ── Step 4: Greedy match formation ───────────────────────────────────
    'outer: for anchor_pos in priority {
        if used[anchor_pos] {
            continue;
        }

        let anchor = &by_mmr[anchor_pos];

        // Re-check atomic state — another worker may have grabbed them
        if !anchor.is_waiting() {
            used[anchor_pos] = true;
            continue;
        }

        // ── Time-Based Constraint Relaxation ─────────────────────────────
        // The longer a player waits, the wider their acceptable MMR range.
        // New player:    ±150 MMR
        // After 6 secs:  ±300 MMR
        // After 18 secs: ±600 MMR (maximum)
        let range = anchor.effective_range(
            cfg.base_mmr_range,
            cfg.relaxation_rate_per_sec,
            cfg.max_mmr_range,
        );

        let lo = anchor.mmr - range;
        let hi = anchor.mmr + range;

        // ── Binary search for the MMR window bounds ───────────────────────
        // O(log n) — much faster than scanning the whole array
        let start = by_mmr.partition_point(|p| p.mmr < lo);
        let end   = by_mmr.partition_point(|p| p.mmr <= hi);

        // Collect available candidates inside the window
        let avail: Vec<usize> = (start..end)
            .filter(|&i| !used[i] && by_mmr[i].is_waiting())
            .collect();

        // Not enough players in range yet — try next anchor
        if avail.len() < 10 {
            continue;
        }

        // ── Pick the 10 closest to anchor's MMR ──────────────────────────
        // Minimizes skill spread within the match
        let mut ranked = avail;
        ranked.sort_unstable_by(|&a, &b| {
            let da = (by_mmr[a].mmr - anchor.mmr).abs();
            let db = (by_mmr[b].mmr - anchor.mmr).abs();
            da.partial_cmp(&db)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked.truncate(10);

        // ── 2-Phase Atomic Reservation ────────────────────────────────────
        // Phase 1: Try to CAS every player from WAITING → RESERVED
        // If ANY player fails (grabbed by another worker), release all
        // and move on. This guarantees each player matches exactly once.
        let mut reserved: Vec<usize> = Vec::with_capacity(10);

        for &idx in &ranked {
            if by_mmr[idx].try_reserve() {
                reserved.push(idx);
            } else {
                // Another worker got this player — undo everything
                for &j in &reserved {
                    by_mmr[j].release();
                }
                continue 'outer;
            }
        }

        // Phase 2: We own all 10 — now commit the match
        let group: Vec<&Arc<PlayerEntry>> =
            reserved.iter().map(|&i| &by_mmr[i]).collect();

        // ── Team Balancing ────────────────────────────────────────────────
        let (team_a_idx, team_b_idx) = balance_teams(&group);

        // Calculate match statistics
        let team_a_mmr: f64 =
            team_a_idx.iter().map(|&i| group[i].mmr).sum::<f64>() / 5.0;
        let team_b_mmr: f64 =
            team_b_idx.iter().map(|&i| group[i].mmr).sum::<f64>() / 5.0;

        let (min_mmr, max_mmr) = group.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(lo, hi), p| (lo.min(p.mmr), hi.max(p.mmr)),
        );

        let spread   = max_mmr - min_mmr;
        let mmr_diff = (team_a_mmr - team_b_mmr).abs();

        // Quality: 1.0 = perfect balance, approaches 0.0 as teams diverge
        let quality  = 1.0 - (mmr_diff / (spread + 1.0)).min(1.0);
        let avg_wait = group.iter().map(|p| p.wait_secs()).sum::<f64>() / 10.0;

        // Confirm all 10 players as MATCHED and remove from pool
        for p in &group {
            p.confirm_match();
        }
        let ids: Vec<String> = group.iter()
            .map(|p| p.player_id.clone())
            .collect();
        pool.evict_matched(&ids);

        // Mark as used in local snapshot
        for &i in &reserved {
            used[i] = true;
        }

        // ── Build and store the match record ─────────────────────────────
        let game_match = GameMatch {
            match_id: new_match_id(),
            team_a: team_a_idx.iter().map(|&i| MatchedPlayer {
                player_id: group[i].player_id.clone(),
                mmr:       group[i].mmr,
                region:    group[i].region.clone(),
            }).collect(),
            team_b: team_b_idx.iter().map(|&i| MatchedPlayer {
                player_id: group[i].player_id.clone(),
                mmr:       group[i].mmr,
                region:    group[i].region.clone(),
            }).collect(),
            team_a_avg_mmr: team_a_mmr,
            team_b_avg_mmr: team_b_mmr,
            mmr_spread:     spread,
            quality_score:  quality,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        metrics.record_match(quality, avg_wait);

        {
            let mut m = recent.write();
            if m.len() >= 200 {
                m.remove(0);
            }
            m.push(game_match);
        }

        formed += 1;
    }

    formed
}

// ─── Team Balancing Algorithm ─────────────────────────────────────────────────
//
// Given 10 players, find the split into 2 teams of 5 that
// minimizes the difference in average MMR between the teams.
//
// Method: Exhaustive search over ALL C(10,5) = 252 possible splits.
//
// How: Use a 10-bit bitmask. If bit j is set → player j is on Team A.
//      We only keep masks with exactly 5 bits set (252 of them).
//      Pick the mask where |sum(A) - sum(B)| is smallest.
//
// Why not approximate?
//   252 iterations × 10 players = 2,520 operations per match.
//   At 1000 matches/sec that's only 2.5M ops/sec — trivially fast.
//   Exhaustive = guaranteed optimal. No approximation needed.

fn balance_teams(
    group: &[&Arc<PlayerEntry>],
) -> (Vec<usize>, Vec<usize>) {
    assert_eq!(group.len(), 10);

    let mmrs: Vec<f64> = group.iter().map(|p| p.mmr).collect();
    let total: f64     = mmrs.iter().sum();
    let half           = total / 2.0;

    let mut best_diff = f64::MAX;
    let mut best_mask = 0u16;

    // Check all 1024 possible 10-bit patterns
    for mask in 0u16..1024 {
        // Only consider patterns with exactly 5 bits set (Team A has 5 players)
        if mask.count_ones() != 5 {
            continue;
        }

        // Sum the MMRs of players assigned to Team A
        let sum_a: f64 = (0..10)
            .filter(|&j| mask & (1 << j) != 0)
            .map(|j| mmrs[j])
            .sum();

        let diff = (sum_a - half).abs();

        if diff < best_diff {
            best_diff = diff;
            best_mask = mask;

            // Perfect balance found — no need to check remaining masks
            if diff == 0.0 {
                break;
            }
        }
    }

    // Build the final team index lists from the winning mask
    let team_a: Vec<usize> = (0..10)
        .filter(|&j| best_mask & (1 << j) != 0)
        .collect();
    let team_b: Vec<usize> = (0..10)
        .filter(|&j| best_mask & (1 << j) == 0)
        .collect();

    (team_a, team_b)
}