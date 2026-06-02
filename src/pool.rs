use dashmap::DashMap;
use std::sync::Arc;
use crate::player::PlayerEntry;

// ─────────────────────────────────────────────────────────────────────────────
// PlayerPool — the central waiting room for all queued players
//
// Backed by DashMap which shards keys across 64 buckets.
// Each bucket has its own RwLock, so concurrent operations on
// different players never block each other.
//
// Complexity:
//   join()             → O(1)  — hash lookup + single bucket lock
//   leave()            → O(1)  — hash lookup + single bucket lock
//   snapshot_waiting() → O(n)  — full scan (called once per cycle)
//   evict_matched()    → O(k)  — k = 10 players per match
// ─────────────────────────────────────────────────────────────────────────────

pub struct PlayerPool {
    inner: DashMap<String, Arc<PlayerEntry>>,
}

impl PlayerPool {
    /// Create a new empty pool.
    /// Pre-allocated for 16,384 players to avoid early rehashing.
    pub fn new() -> Self {
        Self {
            inner: DashMap::with_capacity(16_384),
        }
    }

    /// Add a player to the queue.
    /// Returns true  → player was added successfully.
    /// Returns false → player_id already exists in the queue.
    pub fn join(&self, entry: PlayerEntry) -> bool {
        // DashMap's entry API lets us check + insert atomically
        use dashmap::mapref::entry::Entry;
        match self.inner.entry(entry.player_id.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(entry));
                true
            }
            Entry::Occupied(_) => false, // already in queue
        }
    }

    /// Remove a player who chose to leave before matching.
    /// Returns true  → player was found and removed.
    /// Returns false → player was not in the queue.
    pub fn leave(&self, player_id: &str) -> bool {
        self.inner.remove(player_id).is_some()
    }

    /// Take a snapshot of all players currently in WAITING state.
    /// Workers call this at the start of every matching cycle.
    ///
    /// We clone the Arc (cheap — just increments a counter),
    /// so workers get their own list without holding any locks.
    pub fn snapshot_waiting(&self) -> Vec<Arc<PlayerEntry>> {
        self.inner
            .iter()
            .filter(|entry| entry.value().is_waiting())
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Remove players after a successful match (bulk eviction).
    /// Called with exactly 10 player IDs after every match formed.
    /// Missing entries are silently ignored (already evicted is fine).
    pub fn evict_matched(&self, player_ids: &[String]) {
        for id in player_ids {
            self.inner.remove(id);
        }
    }

    /// Total players in the queue right now (all states).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    
}