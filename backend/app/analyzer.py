from collections import Counter
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging
import json

from sqlalchemy import text

from app.database import AsyncSessionLocal
from app.weights import (
    RANK_WEIGHTS,
    get_combined_weight,
    compute_quality_score,
    IDEAL_GAMES_TARGET
)

logger = logging.getLogger(__name__)

# Mapping des IDs d'items connus
BOOTS_ITEM_IDS = {
    3006,  # Berserker's Greaves
    3009,  # Boots of Swiftness
    3020,  # Sorcerer's Shoes
    3047,  # Plated Steelcaps
    3111,  # Mercury's Treads
    3117,  # Mobility Boots
    3158,  # Ionian Boots of Lucidity
}

STARTING_ITEM_IDS = {
    1054,  # Doran's Shield
    1055,  # Doran's Blade
    1056,  # Doran's Ring
    1082,  # Dark Seal
    1083,  # Cull
    2003,  # Health Potion
    2010,  # Total Biscuit
    2031,  # Refillable Potion
    2033,  # Corrupting Potion
    2051,  # Guardian's Horn
    2052,  # Poro-Snax
    3070,  # Tear of the Goddess
    3850,  # Spellthief's Edge
    3851,  # Frostfang
    3854,  # Steel Shoulderguards
    3855,  # Runesteel Spaulders
    3858,  # Relic Shield
    3859,  # Targon's Buckler
    3862,  # Spectral Sickle
    3863,  # Harrowing Crescent
}

# Mapping des rune paths
RUNE_PATHS = {
    8000: "Precision",
    8100: "Domination",
    8200: "Sorcery",
    8300: "Inspiration",
    8400: "Resolve"
}

# Mapping des summoner spells
SUMMONER_SPELLS = {
    1: "Cleanse",
    3: "Exhaust",
    4: "Flash",
    6: "Ghost",
    7: "Heal",
    11: "Smite",
    12: "Teleport",
    13: "Clarity",
    14: "Ignite",
    21: "Barrier",
    32: "Mark"
}


class BuildAnalyzer:
    """Analyze builds from top player matches"""

    def __init__(self):
        self.skill_map = {1: "Q", 2: "W", 3: "E", 4: "R"}

    async def analyze_builds(
        self,
        players: List[Dict],
        champion: str,
        role: str
    ) -> Dict:
        """
        Aggregate builds from top players
        Returns most popular: runes, items, summoners, skills
        """
        logger.info(f"Analyzing builds for {champion} {role}...")

        all_runes = []
        all_items = []
        all_summoners = []
        all_boots = []
        all_starting = []
        total_games = 0
        total_wins = 0
        game_durations = []

        for player in players:
            for match in player.get("matches", []):
                try:
                    # Find the participant
                    participant = None
                    for p in match["info"]["participants"]:
                        if p["puuid"] == player["puuid"]:
                            participant = p
                            break

                    if not participant:
                        continue

                    # Extract data
                    runes = self._extract_runes(participant)
                    if runes:
                        all_runes.append(tuple(runes))

                    items = self._extract_core_items(participant)
                    if items:
                        all_items.append(tuple(sorted(items)))

                    summoners = (participant["summoner1Id"], participant["summoner2Id"])
                    # Normalize summoner order (Flash always first if present)
                    if summoners[1] == 4:
                        summoners = (4, summoners[0])
                    all_summoners.append(summoners)

                    boots = self._extract_boots(participant)
                    if boots:
                        all_boots.append(boots)

                    starting = self._extract_starting_items(participant)
                    if starting:
                        all_starting.append(tuple(sorted(starting)))

                    total_games += 1
                    if participant["win"]:
                        total_wins += 1

                    # Game duration in seconds
                    game_durations.append(match["info"].get("gameDuration", 0))

                except Exception as e:
                    logger.warning(f"Error processing match: {e}")
                    continue

        if total_games == 0:
            raise Exception("No valid matches found for analysis")

        # Find most common configurations
        winrate = round(total_wins / total_games * 100, 2)
        avg_duration = round(sum(game_durations) / len(game_durations)) if game_durations else 0

        # Build result
        result = {
            "champion": champion,
            "role": role,
            "stats": {
                "winrate": winrate,
                "games_analyzed": total_games,
                "avg_game_duration": avg_duration
            },
            "items": self._build_items_result(all_items, all_boots, all_starting),
            "runes": self._build_runes_result(all_runes),
            "summoner_spells": self._build_summoners_result(all_summoners),
            "skill_order": self._get_skill_order(role),
            "source": "high_elo_analysis"
        }

        logger.info(f"Analysis complete: {total_games} games, {winrate}% WR")
        return result

    async def aggregate_weighted_builds(
        self,
        champion: str,
        role: str,
        current_patch: str,
        previous_patch: Optional[str] = None
    ) -> Dict:
        """
        Aggregate builds from game_records with weighting by rank and age.

        Args:
            champion: Champion name (lowercase)
            role: Role (top, jungle, mid, adc, support)
            current_patch: Current patch version (e.g., "14.2")
            previous_patch: Previous patch version (optional)

        Returns:
            Dict with weighted build data and metrics
        """
        logger.info(f"Aggregating weighted builds for {champion} {role}...")

        async with AsyncSessionLocal() as db:
            # Build query to get games from current and previous patch
            patches = [current_patch]
            if previous_patch:
                patches.append(previous_patch)

            result = await db.execute(
                text("""
                    SELECT
                        id, match_id, rank, patch, game_date, win,
                        game_duration, runes, items, summoner_spells
                    FROM game_records
                    WHERE champion = :champion
                      AND role = :role
                      AND patch = ANY(:patches)
                    ORDER BY game_date DESC
                """),
                {"champion": champion, "role": role, "patches": patches}
            )
            games = result.fetchall()

        if not games:
            raise Exception(f"No games found for {champion} {role}")

        # Process games with weighting
        weighted_games = []
        games_by_rank = {"CHALLENGER": 0, "GRANDMASTER": 0, "MASTER": 0, "DIAMOND": 0}
        total_age_days = 0
        recent_games_7d = 0
        oldest_game_date = None
        newest_game_date = None

        now = datetime.utcnow()

        for game in games:
            game_id, match_id, rank, patch, game_date, win, duration, runes, items, summoners = game

            # Calculate age in days
            age_days = (now - game_date).days if game_date else 0
            is_prev_patch = patch == previous_patch

            # Calculate combined weight
            weight = get_combined_weight(rank, age_days, is_prev_patch)

            weighted_games.append({
                "match_id": match_id,
                "rank": rank,
                "patch": patch,
                "game_date": game_date,
                "win": win,
                "duration": duration,
                "runes": runes,
                "items": items,
                "summoners": summoners,
                "weight": weight,
                "age_days": age_days
            })

            # Update statistics
            games_by_rank[rank] = games_by_rank.get(rank, 0) + 1
            total_age_days += age_days

            if age_days <= 7:
                recent_games_7d += 1

            if oldest_game_date is None or game_date < oldest_game_date:
                oldest_game_date = game_date
            if newest_game_date is None or game_date > newest_game_date:
                newest_game_date = game_date

        total_games = len(weighted_games)
        avg_age_days = total_age_days / total_games if total_games > 0 else 0

        # Calculate weighted statistics
        weighted_winrate = self._compute_weighted_winrate(weighted_games)
        weighted_pickrate = self._compute_weighted_pickrate(weighted_games, games_by_rank)

        # Compute quality score
        quality_score = compute_quality_score(total_games, games_by_rank, avg_age_days)

        # Aggregate build data with weighting
        build_data = self._aggregate_weighted_build_data(weighted_games, champion, role)

        logger.info(
            f"Weighted aggregation complete: {total_games} games, "
            f"{weighted_winrate:.1f}% weighted WR, quality score {quality_score}"
        )

        return {
            "build": build_data,
            "total_games": total_games,
            "games_by_rank": games_by_rank,
            "recent_games_7d": recent_games_7d,
            "oldest_game": oldest_game_date,
            "newest_game": newest_game_date,
            "current_patch": current_patch,
            "quality_score": quality_score,
            "weighted_winrate": round(weighted_winrate, 2),
            "weighted_pickrate": round(weighted_pickrate, 2),
            "avg_age_days": round(avg_age_days, 1)
        }

    def _compute_weighted_winrate(self, weighted_games: List[Dict]) -> float:
        """
        Calculate weighted winrate.
        Formula: Σ(win × weight) / Σ(weight) × 100
        """
        total_weight = sum(g["weight"] for g in weighted_games)
        if total_weight == 0:
            return 0.0

        weighted_wins = sum(
            (1.0 if g["win"] else 0.0) * g["weight"]
            for g in weighted_games
        )

        return (weighted_wins / total_weight) * 100

    def _compute_weighted_pickrate(
        self,
        weighted_games: List[Dict],
        games_by_rank: Dict[str, int]
    ) -> float:
        """
        Calculate weighted pickrate based on rank distribution.
        Higher ranks contribute more to the effective pickrate.
        """
        total_games = len(weighted_games)
        if total_games == 0:
            return 0.0

        # Weighted contribution by rank
        weighted_contribution = sum(
            games_by_rank.get(rank, 0) * RANK_WEIGHTS.get(rank, 0.7)
            for rank in games_by_rank
        )

        # Normalize to a percentage based on target
        return min((weighted_contribution / IDEAL_GAMES_TARGET) * 100, 100)

    def _aggregate_weighted_build_data(
        self,
        weighted_games: List[Dict],
        champion: str,
        role: str
    ) -> Dict:
        """
        Aggregate build data with weighting.
        Items/runes/summoners that appear in higher-weighted games
        contribute more to the final build.
        """
        # Weighted counters
        rune_weights = Counter()
        item_weights = Counter()
        summoner_weights = Counter()
        boots_weights = Counter()
        starting_weights = Counter()

        total_weighted_duration = 0.0
        total_weight = 0.0

        for game in weighted_games:
            weight = game["weight"]
            total_weight += weight

            # Runes
            if game["runes"]:
                runes = game["runes"]
                if isinstance(runes, str):
                    runes = json.loads(runes)
                rune_key = self._runes_to_key(runes)
                if rune_key:
                    rune_weights[rune_key] += weight

            # Items
            if game["items"]:
                items = game["items"]
                if isinstance(items, str):
                    items = json.loads(items)
                for item_id in items.get("core", []):
                    item_weights[item_id] += weight
                for item_id in items.get("full_build", []):
                    item_weights[item_id] += weight * 0.5  # Lower weight for full build items
                if items.get("boots"):
                    boots_weights[items["boots"]] += weight
                if items.get("starting"):
                    starting_key = tuple(sorted(items["starting"]))
                    starting_weights[starting_key] += weight

            # Summoners
            if game["summoners"]:
                summoners = game["summoners"]
                if isinstance(summoners, str):
                    summoners = json.loads(summoners)
                if summoners.get("ids"):
                    spell_key = tuple(sorted(summoners["ids"]))
                    summoner_weights[spell_key] += weight

            # Duration
            if game["duration"]:
                total_weighted_duration += game["duration"] * weight

        avg_duration = int(total_weighted_duration / total_weight) if total_weight > 0 else 0

        # Get weighted winrate for stats
        weighted_winrate = self._compute_weighted_winrate(weighted_games)

        # Build the result using weighted most common
        result = {
            "champion": champion,
            "role": role,
            "stats": {
                "winrate": round(weighted_winrate, 2),
                "games_analyzed": len(weighted_games),
                "avg_game_duration": avg_duration
            },
            "items": self._build_weighted_items_result(
                item_weights, boots_weights, starting_weights, total_weight
            ),
            "runes": self._build_weighted_runes_result(rune_weights, total_weight),
            "summoner_spells": self._build_weighted_summoners_result(summoner_weights, total_weight),
            "skill_order": self._get_skill_order(role),
            "source": "high_elo_weighted_analysis"
        }

        return result

    def _runes_to_key(self, runes: Dict) -> Optional[Tuple]:
        """Convert runes dict to a hashable key tuple"""
        try:
            primary = runes.get("primary", {})
            secondary = runes.get("secondary", {})
            shards = runes.get("shards", {})

            return (
                primary.get("path_id", 0),
                primary.get("keystone", 0),
                *primary.get("slots", [0, 0, 0]),
                secondary.get("path_id", 0),
                *secondary.get("slots", [0, 0]),
                shards.get("offense", 0),
                shards.get("flex", 0),
                shards.get("defense", 0)
            )
        except Exception:
            return None

    def _build_weighted_items_result(
        self,
        item_weights: Counter,
        boots_weights: Counter,
        starting_weights: Counter,
        total_weight: float
    ) -> Dict:
        """Build items result using weighted counters"""
        # Get top 6 items by weight
        core_items = [item for item, _ in item_weights.most_common(6)]

        # Best boots
        boots = boots_weights.most_common(1)
        boots_id = boots[0][0] if boots else None

        # Best starting items
        starting = starting_weights.most_common(1)
        starting_items = list(starting[0][0]) if starting else []

        # Situational items (items with 10-40% weighted appearance)
        situational = []
        if total_weight > 0:
            for item, weight in item_weights.items():
                rate = weight / total_weight
                if 0.1 <= rate <= 0.4 and item not in core_items[:3]:
                    situational.append(item)
            situational = situational[:5]

        return {
            "starting": starting_items,
            "core": core_items[:3],
            "boots": boots_id,
            "full_build": core_items,
            "situational": situational
        }

    def _build_weighted_runes_result(
        self,
        rune_weights: Counter,
        total_weight: float
    ) -> Dict:
        """Build runes result using weighted counters"""
        if not rune_weights:
            return {}

        most_common = rune_weights.most_common(1)
        if not most_common:
            return {}

        runes = most_common[0][0]
        weight = most_common[0][1]
        pickrate = round((weight / total_weight) * 100, 1) if total_weight > 0 else 0

        return {
            "primary": {
                "path": RUNE_PATHS.get(runes[0], "Unknown"),
                "path_id": runes[0],
                "keystone": runes[1],
                "slots": [runes[2], runes[3], runes[4]]
            },
            "secondary": {
                "path": RUNE_PATHS.get(runes[5], "Unknown"),
                "path_id": runes[5],
                "slots": [runes[6], runes[7]]
            },
            "shards": {
                "offense": runes[8],
                "flex": runes[9],
                "defense": runes[10]
            },
            "pickrate": pickrate
        }

    def _build_weighted_summoners_result(
        self,
        summoner_weights: Counter,
        total_weight: float
    ) -> Dict:
        """Build summoner spells result using weighted counters"""
        if not summoner_weights:
            return {}

        most_common = summoner_weights.most_common(1)
        if not most_common:
            return {}

        spells = most_common[0][0]
        weight = most_common[0][1]
        pickrate = round((weight / total_weight) * 100, 1) if total_weight > 0 else 0

        # Normalize order (Flash first if present)
        spell_list = list(spells)
        if 4 in spell_list and spell_list[0] != 4:
            spell_list = [4, spell_list[0] if spell_list[1] == 4 else spell_list[1]]

        return {
            "spell1": {
                "id": spell_list[0],
                "name": SUMMONER_SPELLS.get(spell_list[0], "Unknown")
            },
            "spell2": {
                "id": spell_list[1],
                "name": SUMMONER_SPELLS.get(spell_list[1], "Unknown")
            },
            "ids": spell_list,
            "pickrate": pickrate
        }

    def _extract_runes(self, participant: Dict) -> List[int]:
        """Extract rune IDs from participant"""
        try:
            perks = participant["perks"]["styles"]
            primary = perks[0]["selections"]
            secondary = perks[1]["selections"]
            shards = participant["perks"]["statPerks"]

            return [
                perks[0]["style"],           # Primary tree
                primary[0]["perk"],          # Keystone
                primary[1]["perk"],
                primary[2]["perk"],
                primary[3]["perk"],
                perks[1]["style"],           # Secondary tree
                secondary[0]["perk"],
                secondary[1]["perk"],
                shards["offense"],           # Shards
                shards["flex"],
                shards["defense"]
            ]
        except Exception:
            return []

    def _extract_core_items(self, participant: Dict) -> List[int]:
        """Extract core build items (excluding boots, consumables, starting items)"""
        items = []
        for i in range(6):
            item_id = participant.get(f"item{i}", 0)
            if item_id > 0:
                # Exclude boots, starting items, and low-cost items
                if item_id not in BOOTS_ITEM_IDS and item_id not in STARTING_ITEM_IDS:
                    # Only include completed items (generally > 2000 gold cost)
                    # Item IDs for completed items are typically > 3000
                    if item_id > 2000:
                        items.append(item_id)
        return items

    def _extract_starting_items(self, participant: Dict) -> List[int]:
        """Extract likely starting items based on common patterns"""
        items = []
        for i in range(6):
            item_id = participant.get(f"item{i}", 0)
            if item_id in STARTING_ITEM_IDS:
                items.append(item_id)
        return items

    def _extract_boots(self, participant: Dict) -> Optional[int]:
        """Extract boots item"""
        for i in range(6):
            item_id = participant.get(f"item{i}", 0)
            if item_id in BOOTS_ITEM_IDS:
                return item_id
        return None

    def _build_items_result(
        self,
        all_items: List[Tuple],
        all_boots: List[int],
        all_starting: List[Tuple]
    ) -> Dict:
        """Build the items section of the result"""
        # Core items - find most common item combinations
        item_counter = Counter()
        for items in all_items:
            for item in items:
                item_counter[item] += 1

        # Get top 6 most common items as core
        core_items = [item for item, _ in item_counter.most_common(6)]

        # Boots
        boots_counter = Counter(all_boots)
        most_common_boots = boots_counter.most_common(1)
        boots = most_common_boots[0][0] if most_common_boots else None

        # Starting items
        starting_counter = Counter(all_starting)
        most_common_starting = starting_counter.most_common(1)
        starting = list(most_common_starting[0][0]) if most_common_starting else []

        # Situational items (items that appear in 10-40% of games)
        total_games = len(all_items)
        situational = []
        if total_games > 0:
            for item, count in item_counter.items():
                rate = count / total_games
                if 0.1 <= rate <= 0.4 and item not in core_items[:3]:
                    situational.append(item)
            situational = situational[:5]  # Max 5 situational items

        return {
            "starting": starting,
            "core": core_items[:3],  # First 3 core items
            "boots": boots,
            "full_build": core_items,
            "situational": situational
        }

    def _build_runes_result(self, all_runes: List[Tuple]) -> Dict:
        """Build the runes section of the result"""
        if not all_runes:
            return {}

        rune_counter = Counter(all_runes)
        most_common = rune_counter.most_common(1)

        if not most_common:
            return {}

        runes = most_common[0][0]

        # Calculate pick rate of this rune page
        total = sum(rune_counter.values())
        pickrate = round(most_common[0][1] / total * 100, 1) if total > 0 else 0

        return {
            "primary": {
                "path": RUNE_PATHS.get(runes[0], "Unknown"),
                "path_id": runes[0],
                "keystone": runes[1],
                "slots": [runes[2], runes[3], runes[4]]
            },
            "secondary": {
                "path": RUNE_PATHS.get(runes[5], "Unknown"),
                "path_id": runes[5],
                "slots": [runes[6], runes[7]]
            },
            "shards": {
                "offense": runes[8],
                "flex": runes[9],
                "defense": runes[10]
            },
            "pickrate": pickrate
        }

    def _build_summoners_result(self, all_summoners: List[Tuple]) -> Dict:
        """Build the summoner spells section of the result"""
        if not all_summoners:
            return {}

        summoner_counter = Counter(all_summoners)
        most_common = summoner_counter.most_common(1)

        if not most_common:
            return {}

        spells = most_common[0][0]
        total = sum(summoner_counter.values())
        pickrate = round(most_common[0][1] / total * 100, 1) if total > 0 else 0

        return {
            "spell1": {
                "id": spells[0],
                "name": SUMMONER_SPELLS.get(spells[0], "Unknown")
            },
            "spell2": {
                "id": spells[1],
                "name": SUMMONER_SPELLS.get(spells[1], "Unknown")
            },
            "ids": list(spells),
            "pickrate": pickrate
        }

    def _get_skill_order(self, role: str) -> Dict:
        """
        Return skill order placeholder.
        Real implementation would analyze skill level-up events from timeline.
        """
        # Default skill orders by role (general patterns)
        default_orders = {
            "top": "Q>E>W",
            "jungle": "Q>W>E",
            "mid": "Q>W>E",
            "adc": "Q>W>E",
            "support": "E>Q>W"
        }

        return {
            "priority": default_orders.get(role.lower(), "Q>E>W"),
            "note": "Based on common patterns"
        }


# Global instance
analyzer = BuildAnalyzer()
