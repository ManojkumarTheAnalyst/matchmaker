#!/usr/bin/env python3
"""
5v5 Matchmaker Load Simulation Script
======================================
Injects thousands of concurrent player join requests into the matchmaking
engine and reports real-time performance metrics.

Usage:
    python simulation/simulate.py                        # default settings
    python simulation/simulate.py --players 5000         # 5000 players
    python simulation/simulate.py --players 10000 --wave-size 200
"""

import asyncio
import aiohttp
import random
import time
import argparse
import statistics
from dataclasses import dataclass, field
from typing import List

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL = "http://localhost:3000"

# MMR distributions by skill tier (mean, standard_deviation)
TIERS = {
    "Iron":     (300,  75),
    "Bronze":   (600,  100),
    "Silver":   (900,  125),
    "Gold":     (1200, 150),
    "Platinum": (1500, 175),
    "Diamond":  (1800, 200),
    "Master":   (2200, 250),
    "Grandmaster": (2700, 300),
}

REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast", "ap-northeast"]

# ── Stats Tracker ─────────────────────────────────────────────────────────────

@dataclass
class SimStats:
    total_sent:    int = 0
    total_ok:      int = 0
    total_failed:  int = 0
    latencies_ms:  List[float] = field(default_factory=list)
    start_time:    float = field(default_factory=time.time)

    def success_rate(self) -> float:
        if self.total_sent == 0:
            return 0.0
        return (self.total_ok / self.total_sent) * 100

    def elapsed(self) -> float:
        return time.time() - self.start_time

    def throughput(self) -> float:
        elapsed = self.elapsed()
        return self.total_sent / elapsed if elapsed > 0 else 0

# ── Player Generator ──────────────────────────────────────────────────────────

def make_player(index: int) -> dict:
    """Generate a realistic player with random tier and region."""
    tier_name = random.choice(list(TIERS.keys()))
    mean, std  = TIERS[tier_name]
    mmr        = max(100.0, min(9999.0, random.gauss(mean, std)))
    region     = random.choice(REGIONS)
    player_id  = f"sim-player-{index:08d}-{int(time.time()*1000) % 100000}"

    return {
        "player_id": player_id,
        "mmr":       round(mmr, 1),
        "region":    region,
    }

# ── HTTP Workers ──────────────────────────────────────────────────────────────

async def join_player(
    session:    aiohttp.ClientSession,
    player:     dict,
    stats:      SimStats,
):
    """Send one join request and record the result."""
    t0 = time.perf_counter()
    try:
        async with session.post(
            f"{BASE_URL}/player/join",
            json=player,
        ) as resp:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            stats.total_sent += 1

            if resp.status in (200, 201):
                stats.total_ok += 1
                stats.latencies_ms.append(elapsed_ms)
            elif resp.status == 409:
                # Already in queue — not a real failure
                stats.total_ok += 1
            else:
                stats.total_failed += 1

    except Exception:
        stats.total_sent  += 1
        stats.total_failed += 1

async def send_wave(
    session:     aiohttp.ClientSession,
    wave_players: List[dict],
    stats:       SimStats,
):
    """Send a batch of join requests concurrently."""
    tasks = [
        join_player(session, player, stats)
        for player in wave_players
    ]
    await asyncio.gather(*tasks)

# ── Metrics Fetcher ───────────────────────────────────────────────────────────

async def fetch_server_metrics(session: aiohttp.ClientSession) -> dict:
    try:
        async with session.get(
            f"{BASE_URL}/metrics", timeout=aiohttp.ClientTimeout(total=3)
        ) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception:
        pass
    return {}

# ── Reporter ──────────────────────────────────────────────────────────────────

def print_header():
    print("\n" + "=" * 60)
    print("  5v5 MATCHMAKER — LOAD SIMULATION")
    print("=" * 60)

def print_progress(stats: SimStats, server: dict, wave_num: int):
    matches  = server.get("matches_formed", 0)
    quality  = server.get("avg_quality", 0)
    wait_ms  = server.get("avg_wait_ms", 0)
    cycle_us = server.get("avg_cycle_us", 0)
    pool     = server.get("pool_size", "?")  # from health endpoint

    lats = stats.latencies_ms
    p50  = statistics.median(lats) if lats else 0
    p99  = sorted(lats)[int(len(lats) * 0.99)] if len(lats) > 100 else 0

    print(f"\n[Wave {wave_num:>4}] t={stats.elapsed():.1f}s")
    print(f"  Requests  : {stats.total_sent:>7,} sent  "
          f"| {stats.total_ok:>7,} OK  "
          f"| {stats.total_failed:>5,} failed  "
          f"| {stats.success_rate():.1f}% success")
    print(f"  Throughput: {stats.throughput():>7.0f} req/s")
    print(f"  Latency   : p50={p50:.1f}ms  p99={p99:.1f}ms")
    print(f"  Server    : {matches:>6,} matches formed  "
          f"| quality={quality:.4f}  "
          f"| avg_wait={wait_ms:.0f}ms  "
          f"| cycle={cycle_us:.0f}μs")

def print_final_report(stats: SimStats, server: dict):
    lats = sorted(stats.latencies_ms)
    n    = len(lats)

    def pct(p):
        if n == 0:
            return 0.0
        return lats[min(int(n * p / 100), n - 1)]

    print("\n" + "=" * 60)
    print("  FINAL SIMULATION REPORT")
    print("=" * 60)
    print(f"  Duration          : {stats.elapsed():.1f} seconds")
    print(f"  Total Requests    : {stats.total_sent:,}")
    print(f"  Successful Joins  : {stats.total_ok:,}")
    print(f"  Failed Requests   : {stats.total_failed:,}")
    print(f"  Success Rate      : {stats.success_rate():.2f}%")
    print(f"  Avg Throughput    : {stats.throughput():.0f} requests/second")
    print()
    if lats:
        print(f"  Join Latency:")
        print(f"    p50  : {pct(50):.1f} ms")
        print(f"    p90  : {pct(90):.1f} ms")
        print(f"    p95  : {pct(95):.1f} ms")
        print(f"    p99  : {pct(99):.1f} ms")
        print(f"    max  : {lats[-1]:.1f} ms")
    print()
    print(f"  Server Metrics:")
    print(f"    Matches Formed   : {server.get('matches_formed', 0):,}")
    print(f"    Players Matched  : {server.get('players_matched', 0):,}")
    print(f"    Avg Match Quality: {server.get('avg_quality', 0):.4f} / 1.0000")
    print(f"    Avg Wait Time    : {server.get('avg_wait_ms', 0):.1f} ms")
    print(f"    Avg Cycle Time   : {server.get('avg_cycle_us', 0):.1f} μs")
    print(f"    Total Cycles     : {server.get('cycle_count', 0):,}")
    print("=" * 60)

# ── Main Simulation ───────────────────────────────────────────────────────────

async def run_simulation(
    total_players: int,
    wave_size:     int,
    wave_delay:    float,
):
    print_header()
    print(f"\n  Target     : {BASE_URL}")
    print(f"  Players    : {total_players:,}")
    print(f"  Wave size  : {wave_size}")
    print(f"  Wave delay : {wave_delay}s")

    # Check server is alive
    connector = aiohttp.TCPConnector(limit=500)
    timeout   = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        print("\n  Checking server... ", end="", flush=True)
        try:
            async with session.get(f"{BASE_URL}/health") as r:
                if r.status != 200:
                    print("FAILED — server returned non-200")
                    return
            print("OK ✓")
        except Exception as e:
            print(f"FAILED — {e}")
            print("  Make sure the server is running: cargo run")
            return

        stats    = SimStats()
        wave_num = 0
        sent     = 0

        print(f"\n  Starting injection of {total_players:,} players...\n")

        # Generate all players upfront
        all_players = [make_player(i) for i in range(total_players)]

        while sent < total_players:
            wave_num += 1
            batch     = all_players[sent : sent + wave_size]
            sent     += len(batch)

            await send_wave(session, batch, stats)

            # Every 5 waves, print a progress report
            if wave_num % 5 == 0 or sent >= total_players:
                server = await fetch_server_metrics(session)
                print_progress(stats, server, wave_num)

            if wave_delay > 0 and sent < total_players:
                await asyncio.sleep(wave_delay)

        # Wait for final matches to settle
        print("\n  All players injected. Waiting 5s for final matches...")
        await asyncio.sleep(5)

        # Final report
        server = await fetch_server_metrics(session)
        print_final_report(stats, server)

# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="5v5 Matchmaker Load Simulation"
    )
    parser.add_argument(
        "--players",
        type=int,
        default=5000,
        help="Total players to inject (default: 5000)"
    )
    parser.add_argument(
        "--wave-size",
        type=int,
        default=100,
        help="Players sent per wave (default: 100)"
    )
    parser.add_argument(
        "--wave-delay",
        type=float,
        default=0.05,
        help="Seconds between waves (default: 0.05)"
    )

    args = parser.parse_args()
    asyncio.run(run_simulation(
        total_players=args.players,
        wave_size=args.wave_size,
        wave_delay=args.wave_delay,
    ))

if __name__ == "__main__":
    main()