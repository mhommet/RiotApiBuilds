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
ROLES = ["top", "jungle", "mid", "adc", "support"]


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

        Each champion can appear MULTIPLE TIMES (once per role) with different tiers.
        For example: Akali mid (S tier) and Akali top (B tier).

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
                # Get all recent builds grouped by (champion, role) - each combo is a separate entry
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
                            role,
                            winrate,
                            games_analyzed
                        FROM recent_builds
                        WHERE rn = 1
                          AND games_analyzed >= :min_games
                          AND role IN ('top', 'jungle', 'mid', 'adc', 'support')
                        ORDER BY winrate DESC
                    """),
                    {"rank": rank, "min_games": MIN_GAMES_FOR_TIER}
                )

                rows = result.fetchall()

                if not rows:
                    logger.warning("No recent builds found for tier list generation")
                    return {"S": [], "A": [], "B": [], "C": [], "D": []}

                # Calculate total games for pickrate (across all champion+role combos)
                total_games = sum(row[3] for row in rows if row[3])

                # Build champion+role stats - each (champion, role) is a separate entry
                champion_roles = []
                for row in rows:
                    champion_name = row[0]
                    role = row[1]
                    winrate = float(row[2]) if row[2] else 0.0
                    games = int(row[3]) if row[3] else 0

                    pickrate = (games / total_games * 100) if total_games > 0 else 0

                    # Performance score formula:
                    # Weight winrate heavily, but also consider pickrate
                    # High winrate + high pickrate = very strong
                    # High winrate + low pickrate = potentially strong but niche
                    performance_score = (winrate * 0.7) + (pickrate * 0.3 * 10)

                    champion_roles.append({
                        "champion": champion_name,
                        "role": role,
                        "winrate": round(winrate, 2),
                        "pickrate": round(pickrate, 2),
                        "games_analyzed": games,
                        "performance_score": round(performance_score, 2)
                    })

                # Sort by performance score
                champion_roles.sort(key=lambda x: x["performance_score"], reverse=True)

                # Assign tiers based on percentiles
                total = len(champion_roles)
                tier_list = {"S": [], "A": [], "B": [], "C": [], "D": []}

                for i, entry in enumerate(champion_roles):
                    percentile = (i / total) * 100

                    # S Tier: Top 10% AND (winrate > 52% OR pickrate > 5%)
                    if percentile < 10 and (entry["winrate"] > 52 or entry["pickrate"] > 5):
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

                    entry["tier"] = tier
                    tier_list[tier].append(entry)

                self.champions_analyzed = total
                logger.info(
                    f"Tier list generated (per role): S={len(tier_list['S'])}, A={len(tier_list['A'])}, "
                    f"B={len(tier_list['B'])}, C={len(tier_list['C'])}, D={len(tier_list['D'])} "
                    f"(total entries: {total})"
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
        """Save tier list to database (with role support)"""
        try:
            timestamp = datetime.utcnow()

            # Delete old tier list entries for this rank
            await db.execute(
                text("DELETE FROM tier_list WHERE rank = :rank"),
                {"rank": rank}
            )

            # Insert new tier list entries (one per champion+role combo)
            for tier, entries in tier_list.items():
                for entry in entries:
                    await db.execute(
                        text("""
                            INSERT INTO tier_list
                            (champion, role, tier, rank, winrate, pickrate, games_analyzed, performance_score, timestamp)
                            VALUES (:champion, :role, :tier, :rank, :winrate, :pickrate, :games, :score, :timestamp)
                        """),
                        {
                            "champion": entry["champion"],
                            "role": entry["role"],
                            "tier": tier,
                            "rank": rank,
                            "winrate": entry["winrate"],
                            "pickrate": entry["pickrate"],
                            "games": entry["games_analyzed"],
                            "score": entry["performance_score"],
                            "timestamp": timestamp
                        }
                    )

            await db.commit()
            logger.info(f"Tier list saved to database ({self.champions_analyzed} champion+role entries)")

        except Exception as e:
            logger.error(f"Error saving tier list: {e}")
            await db.rollback()
            raise

    async def get_tier_list(
        self,
        rank: str = "MASTER",
        role: Optional[str] = None
    ) -> Dict[str, List[Dict]]:
        """
        Get the current tier list from database.

        Args:
            rank: Elo rank (MASTER, GRANDMASTER, CHALLENGER)
            role: Optional role filter (top, jungle, mid, adc, support)
                  If None, returns all roles
        """
        try:
            async with AsyncSessionLocal() as db:
                # Build query based on role filter
                if role:
                    query = text("""
                        SELECT champion, role, tier, winrate, pickrate, games_analyzed, performance_score, timestamp
                        FROM tier_list
                        WHERE rank = :rank AND role = :role
                        ORDER BY performance_score DESC
                    """)
                    params = {"rank": rank, "role": role.lower()}
                else:
                    query = text("""
                        SELECT champion, role, tier, winrate, pickrate, games_analyzed, performance_score, timestamp
                        FROM tier_list
                        WHERE rank = :rank
                        ORDER BY performance_score DESC
                    """)
                    params = {"rank": rank}

                result = await db.execute(query, params)
                rows = result.fetchall()

                if not rows:
                    # Try to generate if empty
                    tier_list = await self.generate_tier_list(rank)
                    # If role filter was requested, filter the results
                    if role:
                        role_lower = role.lower()
                        return {
                            tier: [e for e in entries if e.get("role") == role_lower]
                            for tier, entries in tier_list.items()
                        }
                    return tier_list

                tier_list = {"S": [], "A": [], "B": [], "C": [], "D": []}

                for row in rows:
                    tier = row[2]  # tier is now at index 2 (after role)
                    if tier in tier_list:
                        tier_list[tier].append({
                            "champion": row[0],
                            "role": row[1],
                            "tier": tier,
                            "winrate": float(row[3]) if row[3] else 0.0,
                            "pickrate": float(row[4]) if row[4] else 0.0,
                            "games_analyzed": int(row[5]) if row[5] else 0,
                            "performance_score": float(row[6]) if row[6] else 0.0
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
