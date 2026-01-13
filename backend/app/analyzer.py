from collections import Counter
from typing import List, Dict, Optional, Tuple
import logging

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
