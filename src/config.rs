/// All matchmaking settings live here.
/// Change numbers here without touching any other file.
#[derive(Debug, Clone)]
pub struct Config {
    /// How many parallel matching workers run at the same time
    pub num_workers: usize,

    /// How long a worker waits (ms) when pool has fewer than 10 players
    pub idle_sleep_ms: u64,

    /// Starting skill range: a player accepts others within ±150 MMR
    pub base_mmr_range: f64,

    /// Every second of waiting, the range grows by this many MMR points
    /// Example: after 10 seconds → ±(150 + 10×25) = ±400 MMR
    pub relaxation_rate_per_sec: f64,

    /// Maximum skill range allowed, no matter how long they wait
    pub max_mmr_range: f64,

    /// HTTP server port
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            num_workers:             4,
            idle_sleep_ms:           50,
            base_mmr_range:          150.0,
            relaxation_rate_per_sec: 25.0,
            max_mmr_range:           600.0,
            port:                    3000,
        }
    }
}