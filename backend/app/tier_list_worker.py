import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy import text

from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# Tier list configuration
TIER_LIST_INTERVAL = 21600  # Generate tier list every 6 hours
MIN_GAMES_FOR_TIER = 10  # Minimum games to be included in tier list


class TierListWorker:
    """Background worker pour generer la tier list periodiquement"""

    def __init__(self):
        self.running = False
        self.last_generation: Optional[datetime] = None
        self.champions_analyzed = 0
        self.current_status = "idle"

    def get_status(self) -> Dict:
        """Get current tier list worker status"""
        return {
            "running": self.running,
            "status": self.current_status,
            "last_generation": self.last_generation.isoformat() if self.last_generation else None,
            "champions_analyzed": self.champions_analyzed
        }

    async def generate_tier_list(self, rank: str = "MASTER") -> Dict[str, List[Dict]]:
        """
        Generate tier list based on recent builds.

        Tier criteria:
        - S Tier: Top 10% (winrate > 52% + pickrate > 5%)
        - A Tier: 10-25%
        - B Tier: 25-50%
        - C Tier: 50-75%
        - D Tier: Bottom 25%
        """
        self.current_status = f"generating tier list for {rank}"
        logger.info(f"Generating tier list for {rank}...")

        try:
            async with AsyncSessionLocal() as db:
                # Get all recent builds (< 24h) grouped by champion
                result = await db.execute(
                    text("""
                        WITH recent_builds AS (
                            SELECT
                                champion,
                                role,
                                winrate,
                                games_analyzed,
                                data,
                                ROW_NUMBER() OVER (
                                    PARTITION BY champion, role
                                    ORDER BY timestamp DESC
                                ) as rn
                            FROM builds
                            WHERE rank = :rank
                              AND timestamp > NOW() - INTERVAL '48 hours'
                        )
                        SELECT
                            champion,
                            AVG(winrate) as avg_winrate,
                            SUM(games_analyzed) as total_games,
                            COUNT(DISTINCT role) as roles_played,
                            ARRAY_AGG(role) as roles
                        FROM recent_builds
                        WHERE rn = 1
                        GROUP BY champion
                        HAVING SUM(games_analyzed) >= :min_games
                        ORDER BY AVG(winrate) DESC
                    """),
                    {"rank": rank, "min_games": MIN_GAMES_FOR_TIER}
                )

                rows = result.fetchall()

                if not rows:
                    logger.warning("No recent builds found for tier list generation")
                    return {"S": [], "A": [], "B": [], "C": [], "D": []}

                # Calculate total games for pickrate
                total_games = sum(row[2] for row in rows)

                # Build champion stats
                champions = []
                for row in rows:
                    champion_name = row[0]
                    avg_winrate = float(row[1]) if row[1] else 0.0
                    games = int(row[2]) if row[2] else 0
                    roles_played = int(row[3]) if row[3] else 0
                    roles = row[4] if row[4] else []

                    pickrate = (games / total_games * 100) if total_games > 0 else 0

                    # Performance score formula:
                    # Weight winrate heavily, but also consider pickrate
                    # High winrate + high pickrate = very strong
                    # High winrate + low pickrate = potentially strong but niche
                    performance_score = (avg_winrate * 0.7) + (pickrate * 0.3 * 10)

                    champions.append({
                        "champion": champion_name,
                        "winrate": round(avg_winrate, 2),
                        "pickrate": round(pickrate, 2),
                        "games_analyzed": games,
                        "roles_played": roles_played,
                        "roles": list(set(roles)) if roles else [],
                        "performance_score": round(performance_score, 2)
                    })

                # Sort by performance score
                champions.sort(key=lambda x: x["performance_score"], reverse=True)

                # Assign tiers based on percentiles
                total = len(champions)
                tier_list = {"S": [], "A": [], "B": [], "C": [], "D": []}

                for i, champ in enumerate(champions):
                    percentile = (i / total) * 100

                    # S Tier: Top 10% AND (winrate > 52% OR pickrate > 5%)
                    if percentile < 10 and (champ["winrate"] > 52 or champ["pickrate"] > 5):
                        tier = "S"
                    # A Tier: 10-25%
                    elif percentile < 25:
                        tier = "A"
                    # B Tier: 25-50%
                    elif percentile < 50:
                        tier = "B"
                    # C Tier: 50-75%
                    elif percentile < 75:
                        tier = "C"
                    # D Tier: Bottom 25%
                    else:
                        tier = "D"

                    champ["tier"] = tier
                    tier_list[tier].append(champ)

                self.champions_analyzed = total
                logger.info(
                    f"Tier list generated: S={len(tier_list['S'])}, A={len(tier_list['A'])}, "
                    f"B={len(tier_list['B'])}, C={len(tier_list['C'])}, D={len(tier_list['D'])}"
                )

                # Save tier list to database
                await self._save_tier_list(db, tier_list, rank)

                return tier_list

        except Exception as e:
            logger.error(f"Error generating tier list: {e}")
            raise

        finally:
            self.current_status = "idle"

    async def _save_tier_list(
        self,
        db,
        tier_list: Dict[str, List[Dict]],
        rank: str
    ):
        """Save tier list to database"""
        try:
            timestamp = datetime.utcnow()

            # Delete old tier list entries for this rank
            await db.execute(
                text("DELETE FROM tier_list WHERE rank = :rank"),
                {"rank": rank}
            )

            # Insert new tier list entries
            for tier, champions in tier_list.items():
                for champ in champions:
                    await db.execute(
                        text("""
                            INSERT INTO tier_list
                            (champion, tier, rank, winrate, pickrate, games_analyzed, performance_score, timestamp)
                            VALUES (:champion, :tier, :rank, :winrate, :pickrate, :games, :score, :timestamp)
                        """),
                        {
                            "champion": champ["champion"],
                            "tier": tier,
                            "rank": rank,
                            "winrate": champ["winrate"],
                            "pickrate": champ["pickrate"],
                            "games": champ["games_analyzed"],
                            "score": champ["performance_score"],
                            "timestamp": timestamp
                        }
                    )

            await db.commit()
            logger.info(f"Tier list saved to database ({self.champions_analyzed} champions)")

        except Exception as e:
            logger.error(f"Error saving tier list: {e}")
            await db.rollback()
            raise

    async def get_tier_list(self, rank: str = "MASTER") -> Dict[str, List[Dict]]:
        """Get the current tier list from database"""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    text("""
                        SELECT champion, tier, winrate, pickrate, games_analyzed, performance_score, timestamp
                        FROM tier_list
                        WHERE rank = :rank
                        ORDER BY performance_score DESC
                    """),
                    {"rank": rank}
                )

                rows = result.fetchall()

                if not rows:
                    # Try to generate if empty
                    return await self.generate_tier_list(rank)

                tier_list = {"S": [], "A": [], "B": [], "C": [], "D": []}

                for row in rows:
                    tier = row[1]
                    if tier in tier_list:
                        tier_list[tier].append({
                            "champion": row[0],
                            "tier": tier,
                            "winrate": float(row[2]) if row[2] else 0.0,
                            "pickrate": float(row[3]) if row[3] else 0.0,
                            "games_analyzed": int(row[4]) if row[4] else 0,
                            "performance_score": float(row[5]) if row[5] else 0.0
                        })

                return tier_list

        except Exception as e:
            logger.error(f"Error fetching tier list: {e}")
            return {"S": [], "A": [], "B": [], "C": [], "D": []}

    async def run(self):
        """Run the tier list worker in a loop"""
        self.running = True
        logger.info("Tier list worker started")

        # Wait a bit for builds worker to generate some data
        await asyncio.sleep(300)  # Wait 5 minutes before first run

        while self.running:
            try:
                self.current_status = "running"

                # Generate tier list for each rank
                for rank in ["MASTER"]:
                    await self.generate_tier_list(rank)

                self.last_generation = datetime.utcnow()
                self.current_status = "idle"

                logger.info(f"Tier list generation complete. Next run in {TIER_LIST_INTERVAL // 3600} hours")

            except Exception as e:
                logger.error(f"Error in tier list worker: {e}")
                self.current_status = f"error: {str(e)}"

            # Wait for next interval
            await asyncio.sleep(TIER_LIST_INTERVAL)

    def stop(self):
        """Stop the worker"""
        self.running = False
        logger.info("Tier list worker stopping...")


# Global worker instance
tier_list_worker = TierListWorker()
