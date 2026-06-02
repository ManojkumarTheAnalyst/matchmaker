use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;
use serde::{Deserialize, Serialize};

// ─── Player State Machine ───────────────────────────────────────────────────
//
//  WAITING ──► RESERVED ──► MATCHED
//                  │
//                  └──► WAITING  (if match formation fails)
//
//  Only ONE worker can move a player from WAITING→RESERVED.
//  This is enforced by an atomic compare-exchange (CAS) operation.
//  If two workers try at the same time, only ONE wins. The other retries.

pub const STATE_WAITING:  u8 = 0;
pub const STATE_RESERVED: u8 = 1;
pub const STATE_MATCHED:  u8 = 2;

// ─── What the HTTP client sends when joining the queue ──────────────────────

#[derive(Debug, Deserialize)]
pub struct JoinRequest {
    pub player_id: String,
    pub mmr:       f64,

    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "us-east".to_string()
}

// ─── What gets stored in memory for each waiting player ─────────────────────

pub struct PlayerEntry {
    pub player_id: String,
    pub mmr:       f64,
    pub region:    String,

    /// Exact moment this player joined — used to calculate wait time
    pub joined_at: Instant,

    /// The atomic state: WAITING / RESERVED / MATCHED
    /// AtomicU8 means multiple threads can read/write safely with no lock
    pub state: AtomicU8,
}

impl PlayerEntry {
    pub fn new(player_id: String, mmr: f64, region: String) -> Self {
        Self {
            player_id,
            mmr,
            region,
            joined_at: Instant::now(),
            state:     AtomicU8::new(STATE_WAITING),
        }
    }

    /// How many seconds has this player been waiting?
    pub fn wait_secs(&self) -> f64 {
        self.joined_at.elapsed().as_secs_f64()
    }

    /// Current MMR tolerance window for this player.
    /// Grows over time: base + (wait_seconds × rate), capped at max.
    /// Example: after 20s → min(150 + 20×25, 600) = 600 MMR
    pub fn effective_range(&self, base: f64, rate: f64, max: f64) -> f64 {
        (base + self.wait_secs() * rate).min(max)
    }

    /// Try to claim this player for a match.
    /// Uses atomic Compare-And-Swap: only succeeds if state == WAITING.
    /// Returns true for the ONE worker that wins. All others get false.
    pub fn try_reserve(&self) -> bool {
        self.state
            .compare_exchange(
                STATE_WAITING,   // expected current value
                STATE_RESERVED,  // new value if successful
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Release back to WAITING if match formation failed
    pub fn release(&self) {
        self.state.store(STATE_WAITING, Ordering::Release);
    }

    /// Lock in as MATCHED — player is done waiting
    pub fn confirm_match(&self) {
        self.state.store(STATE_MATCHED, Ordering::Release);
    }

    /// Quick check: is this player still available?
    pub fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_WAITING
    }
}

// ─── Lightweight version used inside match results (for HTTP responses) ──────

#[derive(Debug, Clone, Serialize)]
pub struct MatchedPlayer {
    pub player_id: String,
    pub mmr:       f64,
    pub region:    String,
}