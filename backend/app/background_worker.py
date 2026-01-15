import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import aiohttp
from sqlalchemy import text

from app.riot_api import RiotAPI
from app.database import AsyncSessionLocal, update_patch_info, get_current_patch, get_previous_patch
from app.analyzer import analyzer
from app.weights import ALL_RANKS, MIN_GAMES_THRESHOLD, PLAYERS_PER_RANK
import os

logger = logging.getLogger(__name__)

ROLES = ["top", "jungle", "mid", "adc", "support"]

# Timing configuration
DELAY_BETWEEN_COLLECTIONS = 5  # Short delay between API calls
DELAY_BETWEEN_CHAMPIONS = 15  # Delay between champions
DELAY_BETWEEN_CYCLES = 21600  # 6 hours between full cycles


class BuildGeneratorWorker:
    """Background worker pour collecter les games et generer les builds"""

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
        self.games_collected = 0
        self.games_failed = 0
        self.games_skipped = 0  # Already in DB
        self.builds_generated = 0
        self.builds_failed = 0
        self.builds_skipped = 0
        self.cycle_number = 0
        self.cycle_started_at: Optional[datetime] = None
        self.last_update: Optional[datetime] = None
        self.total_builds = 0

        # Rank rotation
        self.rank_index = 0
        self.current_patch: Optional[str] = None

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

    async def get_prioritized_champions(self, all_champions: List[str]) -> List[str]:
        """
        Reorder champions to prioritize those without builds.

        Priority order:
        1. Champions with 0 builds (never processed)
        2. Champions with builds older than 24 hours
        3. Champions with recent builds

        This ensures all champions get at least one build before
        re-scanning those that already have data.
        """
        try:
            async with AsyncSessionLocal() as db:
                # Get champions that have recent builds (< 48h)
                result = await db.execute(
                    text("""
                        SELECT DISTINCT champion
                        FROM builds
                        WHERE timestamp > NOW() - INTERVAL '48 hours'
                    """)
                )
                champions_with_builds = {row[0] for row in result.fetchall()}

                # Get champions with old builds (> 48h but exists)
                result = await db.execute(
                    text("""
                        SELECT DISTINCT champion
                        FROM builds
                        WHERE champion NOT IN (
                            SELECT DISTINCT champion
                            FROM builds
                            WHERE timestamp > NOW() - INTERVAL '48 hours'
                        )
                    """)
                )
                champions_with_old_builds = {row[0] for row in result.fetchall()}

            # Categorize champions
            no_builds = []
            old_builds = []
            recent_builds = []

            for champion in all_champions:
                if champion in champions_with_builds:
                    recent_builds.append(champion)
                elif champion in champions_with_old_builds:
                    old_builds.append(champion)
                else:
                    no_builds.append(champion)

            # Log priority stats
            logger.info(f"=== CHAMPION PRIORITY QUEUE ===")
            logger.info(f"Champions without builds: {len(no_builds)}")
            logger.info(f"Champions with old builds (>48h): {len(old_builds)}")
            logger.info(f"Champions with recent builds: {len(recent_builds)}")

            if no_builds:
                logger.info(f"Priority champions (no builds): {no_builds[:10]}{'...' if len(no_builds) > 10 else ''}")

            # Return prioritized order: no builds → old builds → recent builds
            prioritized = no_builds + old_builds + recent_builds
            return prioritized

        except Exception as e:
            logger.error(f"Error prioritizing champions: {e}")
            # Fallback to original order
            return all_champions

    async def check_and_update_patch(self) -> bool:
        """Check for patch updates and return True if a new patch was detected"""
        try:
            new_patch = await self.riot_api.get_current_patch()
            if new_patch and new_patch != "unknown":
                is_new = await update_patch_info(new_patch)
                self.current_patch = new_patch
                if is_new:
                    logger.info(f"New patch detected: {new_patch}")
                return is_new
        except Exception as e:
            logger.error(f"Error checking patch: {e}")
        return False

    async def get_game_count(self, champion: str, role: str) -> int:
        """Get the number of games stored for a champion/role"""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        SELECT COUNT(*) FROM game_records
                        WHERE champion = :champion AND role = :role
                    """),
                    {"champion": champion, "role": role}
                )
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Error counting games: {e}")
            return 0

    async def should_collect_more(self, champion: str, role: str) -> bool:
        """
        Check if we need to collect more games for this champion/role.
        Returns True if:
        - Less than MIN_GAMES_THRESHOLD games total
        - No games in the last 7 days
        - New patch detected
        """
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        SELECT COUNT(*), MAX(game_date)
                        FROM game_records
                        WHERE champion = :champion AND role = :role
                    """),
                    {"champion": champion, "role": role}
                )
                row = result.fetchone()
                count = row[0] or 0
                latest_date = row[1]

                # Need more if below threshold
                if count < MIN_GAMES_THRESHOLD:
                    return True

                # Need more if data is stale (> 7 days old)
                if latest_date:
                    age = datetime.utcnow() - latest_date
                    if age > timedelta(days=7):
                        return True

                return False

        except Exception as e:
            logger.error(f"Error checking collection need: {e}")
            return True

    async def should_refresh_build(self, champion: str, role: str) -> bool:
        """Check if build aggregation should be refreshed (> 24h or not enough games)"""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        SELECT timestamp FROM builds
                        WHERE champion = :champion AND role = :role
                        ORDER BY timestamp DESC LIMIT 1
                    """),
                    {"champion": champion, "role": role}
                )
                row = result.fetchone()

                if not row:
                    return True

                last_update = row[0]
                age = datetime.utcnow() - last_update
                return age > timedelta(hours=24)

        except Exception as e:
            logger.error(f"Error checking build freshness: {e}")
            return True

    def get_next_rank(self) -> str:
        """Get the next rank in rotation"""
        rank = ALL_RANKS[self.rank_index]
        self.rank_index = (self.rank_index + 1) % len(ALL_RANKS)
        return rank

    async def save_game_record(
        self,
        match_id: str,
        champion: str,
        role: str,
        rank: str,
        patch: str,
        game_date: datetime,
        win: bool,
        game_duration: int,
        runes: Dict,
        items: Dict,
        summoner_spells: Dict
    ) -> bool:
        """
        Save an individual game record.
        Returns True if saved, False if already exists or error.
        """
        try:
            async with AsyncSessionLocal() as db:
                await db.execute(
                    text("""
                        INSERT INTO game_records
                        (match_id, champion, role, rank, patch, game_date, win,
                         game_duration, runes, items, summoner_spells)
                        VALUES (:match_id, :champion, :role, :rank, :patch, :game_date,
                                :win, :duration, CAST(:runes AS jsonb),
                                CAST(:items AS jsonb), CAST(:spells AS jsonb))
                        ON CONFLICT (match_id) DO NOTHING
                    """),
                    {
                        "match_id": match_id,
                        "champion": champion,
                        "role": role,
                        "rank": rank,
                        "patch": patch,
                        "game_date": game_date,
                        "win": win,
                        "duration": game_duration,
                        "runes": json.dumps(runes) if runes else "{}",
                        "items": json.dumps(items) if items else "{}",
                        "spells": json.dumps(summoner_spells) if summoner_spells else "{}"
                    }
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving game record: {e}")
            return False

    async def collect_games_for_champion(self, champion: str, role: str) -> int:
        """
        Collect games for a champion/role from ALL ranks.
        Returns the number of new games collected.
        """
        self.current_task = f"Collecting {champion} {role}"
        logger.info(f"Collecting games for {champion} {role} (all ranks)...")

        if not self.current_patch:
            self.current_patch = await self.riot_api.get_current_patch()

        new_games = 0

        for rank in ALL_RANKS:
            try:
                players = await self.riot_api.get_champion_leaderboard(
                    champion=champion,
                    role=role,
                    rank=rank,
                    limit=PLAYERS_PER_RANK
                )

                for player in players:
                    for match in player.get("matches", []):
                        try:
                            match_id = match.get("match_id")
                            if not match_id:
                                continue

                            # Extract data for storage
                            participant = match.get("participant", {})
                            game_date_str = match.get("game_date")
                            game_date = datetime.fromisoformat(game_date_str) if game_date_str else datetime.utcnow()

                            # Extract runes
                            runes = self._extract_runes_for_storage(participant)

                            # Extract items
                            items = self._extract_items_for_storage(participant)

                            # Extract summoner spells
                            summoner_spells = self._extract_summoners_for_storage(participant)

                            # Save to database
                            saved = await self.save_game_record(
                                match_id=match_id,
                                champion=champion,
                                role=role,
                                rank=rank,
                                patch=self.current_patch,
                                game_date=game_date,
                                win=match.get("win", False),
                                game_duration=match.get("game_duration", 0),
                                runes=runes,
                                items=items,
                                summoner_spells=summoner_spells
                            )

                            if saved:
                                new_games += 1
                                self.games_collected += 1
                            else:
                                self.games_skipped += 1

                        except Exception as e:
                            logger.warning(f"Error processing match: {e}")
                            self.games_failed += 1

                # Small delay between ranks
                await asyncio.sleep(DELAY_BETWEEN_COLLECTIONS)

            except Exception as e:
                logger.error(f"Error collecting {rank} games for {champion} {role}: {e}")

        logger.info(f"Collected {new_games} new games for {champion} {role}")
        return new_games

    def _extract_runes_for_storage(self, participant: Dict) -> Dict:
        """Extract runes from participant for storage"""
        try:
            perks = participant.get("perks", {}).get("styles", [])
            if len(perks) < 2:
                return {}

            primary = perks[0].get("selections", [])
            secondary = perks[1].get("selections", [])
            shards = participant.get("perks", {}).get("statPerks", {})

            return {
                "primary": {
                    "path_id": perks[0].get("style", 0),
                    "keystone": primary[0].get("perk", 0) if primary else 0,
                    "slots": [p.get("perk", 0) for p in primary[1:4]] if len(primary) > 1 else []
                },
                "secondary": {
                    "path_id": perks[1].get("style", 0),
                    "slots": [p.get("perk", 0) for p in secondary[:2]] if secondary else []
                },
                "shards": {
                    "offense": shards.get("offense", 0),
                    "flex": shards.get("flex", 0),
                    "defense": shards.get("defense", 0)
                }
            }
        except Exception:
            return {}

    def _extract_items_for_storage(self, participant: Dict) -> Dict:
        """Extract items from participant for storage"""
        try:
            all_items = []
            boots = None
            starting = []

            BOOTS_IDS = {3006, 3009, 3020, 3047, 3111, 3117, 3158}
            STARTING_IDS = {1054, 1055, 1056, 1082, 1083, 2003, 2031, 2033, 3070, 3850, 3854, 3858, 3862}

            for i in range(6):
                item_id = participant.get(f"item{i}", 0)
                if item_id > 0:
                    if item_id in BOOTS_IDS:
                        boots = item_id
                    elif item_id in STARTING_IDS:
                        starting.append(item_id)
                    elif item_id > 2000:  # Completed items
                        all_items.append(item_id)

            return {
                "core": all_items[:3],
                "full_build": all_items,
                "boots": boots,
                "starting": starting
            }
        except Exception:
            return {}

    def _extract_summoners_for_storage(self, participant: Dict) -> Dict:
        """Extract summoner spells for storage"""
        try:
            spell1 = participant.get("summoner1Id", 0)
            spell2 = participant.get("summoner2Id", 0)

            # Normalize (Flash first)
            if spell2 == 4:
                spell1, spell2 = spell2, spell1

            return {
                "ids": [spell1, spell2]
            }
        except Exception:
            return {}

    async def cleanup_old_games(self):
        """Remove games from patches older than N-1"""
        try:
            current = await get_current_patch()
            previous = await get_previous_patch()

            if not current:
                return

            patches_to_keep = [current]
            if previous:
                patches_to_keep.append(previous)

            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        DELETE FROM game_records
                        WHERE patch NOT IN (SELECT unnest(:patches))
                        RETURNING id
                    """),
                    {"patches": patches_to_keep}
                )
                deleted = result.fetchall()
                await db.commit()

                if deleted:
                    logger.info(f"Cleaned up {len(deleted)} old game records")

        except Exception as e:
            logger.error(f"Error cleaning up old games: {e}")

    async def generate_and_save_build(self, champion: str, role: str) -> bool:
        """
        Generate a weighted aggregated build from game_records and save it.
        Returns True on success.
        """
        try:
            self.current_task = f"Aggregating {champion} {role}"
            logger.info(f"Generating weighted build: {champion} {role}")

            # Get current and previous patch
            current_patch = await get_current_patch()
            previous_patch = await get_previous_patch()

            if not current_patch:
                current_patch = await self.riot_api.get_current_patch()

            # Use the new weighted aggregation
            try:
                aggregated = await analyzer.aggregate_weighted_builds(
                    champion=champion,
                    role=role,
                    current_patch=current_patch,
                    previous_patch=previous_patch
                )
            except Exception as e:
                logger.error(f"Weighted aggregation failed for {champion} {role}: {e}")
                return False

            # Extract data from aggregated result
            build_data = aggregated["build"]
            weighted_winrate = aggregated["weighted_winrate"]
            total_games = aggregated["total_games"]
            games_by_rank = aggregated["games_by_rank"]
            quality_score = aggregated["quality_score"]

            # Save to database with new columns
            async with AsyncSessionLocal() as db:
                await db.execute(
                    text("""
                        INSERT INTO builds (
                            champion, role, rank, data, games_analyzed, winrate,
                            timestamp, total_games_analyzed, games_by_rank, recent_games,
                            oldest_game_date, current_patch, data_quality_score,
                            weighted_winrate, weighted_pickrate
                        )
                        VALUES (
                            :champion, :role, :rank, CAST(:data AS jsonb), :games, :winrate,
                            :timestamp, :total_games, CAST(:games_by_rank AS jsonb), :recent_games,
                            :oldest_game, :current_patch, :quality_score,
                            :weighted_winrate, :weighted_pickrate
                        )
                    """),
                    {
                        "champion": champion,
                        "role": role,
                        "rank": "AGGREGATED",  # Special rank for aggregated builds
                        "data": json.dumps(build_data),
                        "games": total_games,
                        "winrate": weighted_winrate,
                        "timestamp": datetime.utcnow(),
                        "total_games": total_games,
                        "games_by_rank": json.dumps(games_by_rank),
                        "recent_games": aggregated.get("recent_games_7d", 0),
                        "oldest_game": aggregated.get("oldest_game"),
                        "current_patch": current_patch,
                        "quality_score": quality_score,
                        "weighted_winrate": weighted_winrate,
                        "weighted_pickrate": aggregated.get("weighted_pickrate", 0)
                    }
                )
                await db.commit()

            logger.info(
                f"Weighted build saved: {champion} {role} "
                f"({total_games} games, {weighted_winrate}% WR, quality: {quality_score})"
            )
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
            "current_patch": self.current_patch,
            "games_collected": self.games_collected,
            "games_failed": self.games_failed,
            "games_skipped": self.games_skipped,
            "builds_generated": self.builds_generated,
            "builds_failed": self.builds_failed,
            "builds_skipped": self.builds_skipped,
            "total_builds": self.total_builds,
            "progress_percent": progress,
            "cycle_started_at": self.cycle_started_at.isoformat() if self.cycle_started_at else None,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "champions_loaded": len(self.champions),
            "ranks_collected": ALL_RANKS,
            "priority_mode": "missing_first"
        }

    async def run(self):
        """Lance le worker en boucle infinie"""
        self.running = True
        logger.info("Background worker started (multi-rank collection mode)")

        # Fetch champions list and check patch
        self.champions = await self.fetch_all_champions()
        await self.check_and_update_patch()

        self.total_builds = len(self.champions) * len(ROLES)
        logger.info(
            f"Will process {len(self.champions)} champions x {len(ROLES)} roles x {len(ALL_RANKS)} ranks "
            f"= {self.total_builds} aggregated builds"
        )

        while self.running:
            self.cycle_number += 1
            self.cycle_started_at = datetime.utcnow()
            self.games_collected = 0
            self.games_failed = 0
            self.games_skipped = 0
            self.builds_generated = 0
            self.builds_failed = 0
            self.builds_skipped = 0

            logger.info(f"Starting collection cycle #{self.cycle_number} (patch: {self.current_patch})")

            # Check for patch updates
            new_patch = await self.check_and_update_patch()
            if new_patch:
                logger.info("New patch detected - cleaning up old data")
                await self.cleanup_old_games()

            # Prioritize champions without builds (runs at each cycle start)
            prioritized_champions = await self.get_prioritized_champions(self.champions)

            await self.update_worker_stats()

            processed = 0
            for champion in prioritized_champions:
                if not self.running:
                    break

                for role in ROLES:
                    if not self.running:
                        break

                    processed += 1

                    # Phase 1: Check if we need to collect more games
                    need_collection = await self.should_collect_more(champion, role)

                    if need_collection:
                        # Collect games from all ranks
                        new_games = await self.collect_games_for_champion(champion, role)
                        logger.info(
                            f"Collected {new_games} games for {champion} {role}"
                        )
                        await asyncio.sleep(DELAY_BETWEEN_CHAMPIONS)

                    # Phase 2: Check if build aggregation needs refresh
                    need_build = await self.should_refresh_build(champion, role)

                    if need_build:
                        # Check if we have enough games to aggregate
                        game_count = await self.get_game_count(champion, role)

                        if game_count >= 5:  # Minimum games for aggregation
                            success = await self.generate_and_save_build(champion, role)

                            if success:
                                self.builds_generated += 1
                            else:
                                self.builds_failed += 1
                        else:
                            logger.debug(f"Skipping {champion} {role} build (only {game_count} games)")
                            self.builds_skipped += 1

                    else:
                        self.builds_skipped += 1

                    # Update stats periodically
                    if processed % 10 == 0:
                        await self.update_worker_stats()
                        logger.info(
                            f"Progress: {processed}/{self.total_builds} | "
                            f"Games: {self.games_collected} collected, {self.games_skipped} skipped | "
                            f"Builds: {self.builds_generated} generated, {self.builds_skipped} skipped"
                        )

            # Cycle complete - cleanup old games
            await self.cleanup_old_games()
            await self.update_worker_stats()

            cycle_duration = datetime.utcnow() - self.cycle_started_at
            logger.info(
                f"Cycle #{self.cycle_number} complete in {cycle_duration}. "
                f"Games: {self.games_collected} collected, {self.games_failed} failed. "
                f"Builds: {self.builds_generated} generated, {self.builds_skipped} skipped, {self.builds_failed} failed. "
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
