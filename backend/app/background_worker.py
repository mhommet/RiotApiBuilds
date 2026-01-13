import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import aiohttp
from sqlalchemy import text

from app.riot_api import RiotAPI
from app.database import get_db, AsyncSessionLocal
from app.analyzer import analyzer
import os

logger = logging.getLogger(__name__)

ROLES = ["top", "jungle", "mid", "adc", "support"]
RANKS = ["MASTER"]

# Timing configuration
DELAY_BETWEEN_BUILDS = 10  # Short delay, rate limiting handled by riot_api
DELAY_BETWEEN_CYCLES = 21600  # 6 hours between full cycles


class BuildGeneratorWorker:
    """Background worker pour generer les builds progressivement"""

    def __init__(self):
        self.riot_api = RiotAPI(
            api_key=os.getenv("RIOT_API_KEY"),
            region=os.getenv("RIOT_REGION", "euw1"),
            continent=os.getenv("RIOT_CONTINENT", "europe")
        )
        self.running = False
        self.current_task = None
        self.champions: List[str] = []

        # Stats tracking
        self.builds_generated = 0
        self.builds_failed = 0
        self.builds_skipped = 0
        self.cycle_number = 0
        self.cycle_started_at: Optional[datetime] = None
        self.last_update: Optional[datetime] = None
        self.total_builds = 0

    async def fetch_all_champions(self) -> List[str]:
        """Recupere tous les champions depuis Data Dragon"""
        try:
            async with aiohttp.ClientSession() as session:
                # Get latest version
                async with session.get(
                    "https://ddragon.leagueoflegends.com/api/versions.json",
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed to fetch versions: {resp.status}")
                    versions = await resp.json()
                    latest_version = versions[0]

                # Get champions
                champion_url = f"https://ddragon.leagueoflegends.com/cdn/{latest_version}/data/en_US/champion.json"
                async with session.get(
                    champion_url,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed to fetch champions: {resp.status}")
                    data = await resp.json()
                    champions = [champ["id"].lower() for champ in data["data"].values()]

                    logger.info(f"Fetched {len(champions)} champions from Data Dragon (v{latest_version})")
                    return sorted(champions)

        except Exception as e:
            logger.error(f"Error fetching champions: {e}")
            # Fallback to a small list for testing
            return ["aatrox", "ahri", "akali", "yasuo", "yone", "zed", "jinx", "lux"]

    async def should_refresh_build(self, champion: str, role: str, rank: str) -> bool:
        """Verifie si un build doit etre rafraichi (> 24h)"""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        SELECT timestamp FROM builds
                        WHERE champion = :champion AND role = :role AND rank = :rank
                        ORDER BY timestamp DESC LIMIT 1
                    """),
                    {"champion": champion, "role": role, "rank": rank}
                )
                row = result.fetchone()

                if not row:
                    return True

                last_update = row[0]
                age = datetime.utcnow() - last_update
                return age > timedelta(hours=24)

        except Exception as e:
            logger.error(f"Error checking build freshness: {e}")
            return True  # Refresh if we can't check

    async def generate_and_save_build(self, champion: str, role: str, rank: str) -> bool:
        """Genere un build et le sauvegarde en DB. Returns True on success."""
        try:
            self.current_task = f"{champion} {role} {rank}"
            logger.info(f"Generating build: {champion} {role} {rank}")

            # Fetch player data from Riot API
            players = await self.riot_api.get_champion_leaderboard(
                champion=champion,
                role=role,
                rank=rank,
                limit=5  # Small to minimize API calls
            )

            if not players or len(players) == 0:
                logger.warning(f"No data found for {champion} {role}")
                return False

            # Analyze builds using the analyzer
            try:
                build_data = await analyzer.analyze_builds(players, champion, role)
            except Exception as e:
                logger.error(f"Analysis failed for {champion} {role}: {e}")
                return False

            # Extract winrate from build data
            winrate = build_data.get("stats", {}).get("winrate", 0.0)
            games_analyzed = build_data.get("stats", {}).get("games_analyzed", len(players))

            # Save to database
            async with AsyncSessionLocal() as db:
                await db.execute(
                    text("""
                        INSERT INTO builds (champion, role, rank, data, games_analyzed, winrate, timestamp)
                        VALUES (:champion, :role, :rank, CAST(:data AS jsonb), :games, :winrate, :timestamp)
                    """),
                    {
                        "champion": champion,
                        "role": role,
                        "rank": rank,
                        "data": json.dumps(build_data),
                        "games": games_analyzed,
                        "winrate": winrate,
                        "timestamp": datetime.utcnow()
                    }
                )
                await db.commit()

            logger.info(f"Build saved: {champion} {role} ({games_analyzed} games, {winrate}% WR)")
            self.last_update = datetime.utcnow()
            return True

        except Exception as e:
            logger.error(f"Error generating build for {champion} {role}: {e}")
            return False

        finally:
            self.current_task = None

    async def update_worker_stats(self):
        """Update worker stats in database"""
        try:
            async with AsyncSessionLocal() as db:
                # Check if stats row exists
                result = await db.execute(
                    text("SELECT id FROM worker_stats WHERE worker_type = 'build_generator' LIMIT 1")
                )
                row = result.fetchone()

                if row:
                    await db.execute(
                        text("""
                            UPDATE worker_stats SET
                                builds_generated = :builds_generated,
                                builds_total = :builds_total,
                                last_champion = :last_champion,
                                last_role = :last_role,
                                cycle_number = :cycle_number,
                                last_update = :last_update,
                                status = :status
                            WHERE worker_type = 'build_generator'
                        """),
                        {
                            "builds_generated": self.builds_generated,
                            "builds_total": self.total_builds,
                            "last_champion": self.current_task.split()[0] if self.current_task else None,
                            "last_role": self.current_task.split()[1] if self.current_task and len(self.current_task.split()) > 1 else None,
                            "cycle_number": self.cycle_number,
                            "last_update": datetime.utcnow(),
                            "status": "running" if self.running else "stopped"
                        }
                    )
                else:
                    await db.execute(
                        text("""
                            INSERT INTO worker_stats
                            (worker_type, builds_generated, builds_total, cycle_number, started_at, last_update, status)
                            VALUES ('build_generator', :builds_generated, :builds_total, :cycle_number, :started_at, :last_update, :status)
                        """),
                        {
                            "builds_generated": self.builds_generated,
                            "builds_total": self.total_builds,
                            "cycle_number": self.cycle_number,
                            "started_at": self.cycle_started_at,
                            "last_update": datetime.utcnow(),
                            "status": "running" if self.running else "stopped"
                        }
                    )
                await db.commit()
        except Exception as e:
            logger.error(f"Error updating worker stats: {e}")

    def get_status(self) -> Dict:
        """Get current worker status"""
        progress = 0.0
        if self.total_builds > 0:
            progress = round((self.builds_generated + self.builds_skipped) / self.total_builds * 100, 1)

        return {
            "running": self.running,
            "current_task": self.current_task,
            "cycle_number": self.cycle_number,
            "builds_generated": self.builds_generated,
            "builds_failed": self.builds_failed,
            "builds_skipped": self.builds_skipped,
            "total_builds": self.total_builds,
            "progress_percent": progress,
            "cycle_started_at": self.cycle_started_at.isoformat() if self.cycle_started_at else None,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "champions_loaded": len(self.champions)
        }

    async def run(self):
        """Lance le worker en boucle infinie"""
        self.running = True
        logger.info("Background worker started")

        # Fetch champions list
        self.champions = await self.fetch_all_champions()
        self.total_builds = len(self.champions) * len(ROLES) * len(RANKS)
        logger.info(f"Will process {len(self.champions)} champions x {len(ROLES)} roles = {self.total_builds} builds")

        while self.running:
            self.cycle_number += 1
            self.cycle_started_at = datetime.utcnow()
            self.builds_generated = 0
            self.builds_failed = 0
            self.builds_skipped = 0

            logger.info(f"Starting generation cycle #{self.cycle_number}")
            await self.update_worker_stats()

            processed = 0
            for champion in self.champions:
                if not self.running:
                    break

                for role in ROLES:
                    if not self.running:
                        break

                    for rank in RANKS:
                        if not self.running:
                            break

                        processed += 1

                        # Check if build needs refresh
                        should_refresh = await self.should_refresh_build(champion, role, rank)

                        if should_refresh:
                            success = await self.generate_and_save_build(champion, role, rank)

                            if success:
                                self.builds_generated += 1
                            else:
                                self.builds_failed += 1

                            # Update stats periodically
                            if processed % 10 == 0:
                                await self.update_worker_stats()

                            # Wait between builds to respect rate limits
                            logger.info(
                                f"Progress: {processed}/{self.total_builds} "
                                f"({self.builds_generated} generated, {self.builds_skipped} skipped, {self.builds_failed} failed) "
                                f"| Waiting {DELAY_BETWEEN_BUILDS}s..."
                            )
                            await asyncio.sleep(DELAY_BETWEEN_BUILDS)
                        else:
                            self.builds_skipped += 1
                            logger.debug(f"Skipping {champion} {role} (cache valid)")

            # Cycle complete
            await self.update_worker_stats()
            cycle_duration = datetime.utcnow() - self.cycle_started_at
            logger.info(
                f"Cycle #{self.cycle_number} complete in {cycle_duration}. "
                f"Generated: {self.builds_generated}, Skipped: {self.builds_skipped}, Failed: {self.builds_failed}. "
                f"Waiting {DELAY_BETWEEN_CYCLES // 3600} hours for next cycle..."
            )

            # Wait before next cycle
            await asyncio.sleep(DELAY_BETWEEN_CYCLES)

    def stop(self):
        """Arrete le worker"""
        self.running = False
        logger.info("Background worker stopping...")


# Global worker instance
worker = BuildGeneratorWorker()
