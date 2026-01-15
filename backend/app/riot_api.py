import aiohttp
import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Champion name to ID mapping (needed for mastery endpoint)
# Will be fetched from Data Dragon on init
CHAMPION_IDS: Dict[str, int] = {}

# Supported ranks
SUPPORTED_RANKS = ["CHALLENGER", "GRANDMASTER", "MASTER", "DIAMOND"]


class RiotAPI:
    """Client for Riot Games API - optimized with champion mastery check"""

    def __init__(self, api_key: str, region: str = "euw1", continent: str = "europe"):
        self.api_key = api_key
        self.region = region
        self.continent = continent
        self.base_url = f"https://{region}.api.riotgames.com"
        self.continent_url = f"https://{continent}.api.riotgames.com"
        self.headers = {
            "X-Riot-Token": api_key,
            "Accept-Language": "en-US,en;q=0.9"
        }
        self.champion_ids: Dict[str, int] = {}
        self._current_patch: Optional[str] = None

    async def _request(self, url: str, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Make request with retry on rate limit"""
        max_retries = 5

        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    elif resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 10)) + 1
                        logger.warning(f"Rate limited, waiting {retry_after}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(retry_after)
                        continue

                    elif resp.status == 404:
                        return None

                    else:
                        logger.error(f"API Error {resp.status}")
                        return None

            except asyncio.TimeoutError:
                logger.warning(f"Timeout (attempt {attempt + 1})")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Request error: {e}")
                await asyncio.sleep(2)

        return None

    async def _ensure_champion_ids(self, session: aiohttp.ClientSession):
        """Load champion IDs from Data Dragon if not loaded"""
        if self.champion_ids:
            return

        try:
            async with session.get("https://ddragon.leagueoflegends.com/api/versions.json") as resp:
                versions = await resp.json()
                version = versions[0]

            # Extract major.minor patch version (e.g., "14.2.1" -> "14.2")
            parts = version.split(".")
            self._current_patch = f"{parts[0]}.{parts[1]}"

            url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
            async with session.get(url) as resp:
                data = await resp.json()
                for champ in data["data"].values():
                    self.champion_ids[champ["id"].lower()] = int(champ["key"])

            logger.info(f"Loaded {len(self.champion_ids)} champion IDs (patch {self._current_patch})")
        except Exception as e:
            logger.error(f"Failed to load champion IDs: {e}")

    async def get_current_patch(self) -> str:
        """
        Get the current LoL patch version from Data Dragon.
        Returns format: "14.2" (major.minor)
        """
        if self._current_patch:
            return self._current_patch

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get("https://ddragon.leagueoflegends.com/api/versions.json") as resp:
                    versions = await resp.json()
                    full_version = versions[0]
                    parts = full_version.split(".")
                    self._current_patch = f"{parts[0]}.{parts[1]}"
                    logger.info(f"Current patch: {self._current_patch}")
                    return self._current_patch
            except Exception as e:
                logger.error(f"Failed to fetch patch version: {e}")
                return "unknown"

    async def _get_diamond_players(self, session: aiohttp.ClientSession) -> List[Dict]:
        """
        Get Diamond I players using the league entries endpoint.
        Diamond uses a different API structure than Challenger/Grandmaster/Master.
        """
        all_entries = []

        # Diamond I only (highest Diamond division)
        for page in range(1, 3):  # Get first 2 pages (about 400 players)
            url = f"{self.base_url}/lol/league/v4/entries/RANKED_SOLO_5x5/DIAMOND/I?page={page}"
            data = await self._request(url, session)

            if not data:
                break

            if isinstance(data, list):
                all_entries.extend(data)
            else:
                break

            if len(data) < 205:  # Less than full page, stop
                break

        logger.info(f"Fetched {len(all_entries)} Diamond I players")
        return all_entries

    async def get_champion_leaderboard(
        self,
        champion: str,
        role: str,
        rank: str = "MASTER",
        limit: int = 10
    ) -> List[Dict]:
        """
        Get top players for a champion using mastery-based filtering.
        Supports CHALLENGER, GRANDMASTER, MASTER, and DIAMOND ranks.

        Returns list of players with their matches including metadata.
        """
        logger.info(f"Fetching {champion} {role} players ({rank})...")

        async with aiohttp.ClientSession() as session:
            # Load champion IDs
            await self._ensure_champion_ids(session)
            champion_id = self.champion_ids.get(champion.lower())

            if not champion_id:
                logger.error(f"Unknown champion: {champion}")
                return []

            # Get players based on rank
            if rank == "DIAMOND":
                # Diamond uses a different endpoint structure
                entries = await self._get_diamond_players(session)

                # Diamond entries need PUUID lookup (unlike Challenger/GM/Master)
                entries_with_puuid = []
                puuid_fetch_failed = 0

                for i, entry in enumerate(entries[:100]):  # Limit to 100 to reduce API calls
                    # Check if puuid already exists (newer API versions may include it)
                    if entry.get("puuid"):
                        entries_with_puuid.append(entry)
                        continue

                    summoner_id = entry.get("summonerId")
                    if summoner_id:
                        # Get PUUID from summoner ID
                        summoner_url = f"{self.base_url}/lol/summoner/v4/summoners/{summoner_id}"
                        summoner_data = await self._request(summoner_url, session)

                        if summoner_data and "puuid" in summoner_data:
                            entry["puuid"] = summoner_data["puuid"]
                            entries_with_puuid.append(entry)
                        else:
                            puuid_fetch_failed += 1

                        # Add small delay every 10 requests to avoid rate limiting
                        if (i + 1) % 10 == 0:
                            await asyncio.sleep(0.5)

                if puuid_fetch_failed > 0:
                    logger.warning(f"Failed to fetch PUUID for {puuid_fetch_failed} Diamond players (rate limit or API error)")

                logger.info(f"Diamond players with PUUID: {len(entries_with_puuid)}/{len(entries[:100])}")
                entries = entries_with_puuid
            else:
                # Challenger, Grandmaster, Master use league endpoints
                if rank == "CHALLENGER":
                    url = f"{self.base_url}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
                elif rank == "GRANDMASTER":
                    url = f"{self.base_url}/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5"
                else:  # MASTER
                    url = f"{self.base_url}/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5"

                league_data = await self._request(url, session)
                if not league_data or "entries" not in league_data:
                    logger.error("Failed to fetch league data")
                    return []

                entries = league_data["entries"]

            # Sort by LP to get best players first
            entries.sort(key=lambda x: x.get("leaguePoints", 0), reverse=True)

            logger.info(f"Got {len(entries)} {rank} players, checking mastery for {champion}...")

            champion_players = []
            players_with_mastery = 0
            max_players_to_check = 80 if rank != "DIAMOND" else 50

            for i, player in enumerate(entries[:max_players_to_check]):
                if len(champion_players) >= limit:
                    break

                puuid = player.get('puuid')
                if not puuid:
                    continue

                # Quick mastery check - 1 request instead of many
                mastery_url = f"{self.base_url}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/by-champion/{champion_id}"
                mastery = await self._request(mastery_url, session)

                if not mastery or mastery.get("championPoints", 0) < 15000:
                    continue  # Skip if less than 15k mastery

                players_with_mastery += 1
                logger.info(f"Found player with {mastery.get('championPoints', 0):,} mastery on {champion}")

                # Get their matches with enhanced metadata
                matches = await self._get_champion_matches_enhanced(session, puuid, champion, rank)

                if len(matches) >= 2:
                    champion_players.append({
                        "puuid": puuid,
                        "summoner_name": f"Player{i}",
                        "rank": rank,
                        "lp": player.get("leaguePoints", 0),
                        "mastery": mastery.get("championPoints", 0),
                        "matches": matches
                    })
                    logger.info(f"Added player {len(champion_players)}/{limit}: {len(matches)} recent games")

            logger.info(f"Found {len(champion_players)} {champion} players (checked {players_with_mastery} with mastery)")
            return champion_players

    async def _get_champion_matches(
        self,
        session: aiohttp.ClientSession,
        puuid: str,
        champion: str
    ) -> List[Dict]:
        """Get recent ranked matches for a champion (legacy method)"""
        url = f"{self.continent_url}/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&count=10"
        match_ids = await self._request(url, session)

        if not match_ids:
            return []

        matches = []

        for match_id in match_ids[:8]:
            if len(matches) >= 3:
                break

            match_url = f"{self.continent_url}/lol/match/v5/matches/{match_id}"
            match_data = await self._request(match_url, session)

            if not match_data:
                continue

            for p in match_data["info"]["participants"]:
                if p["puuid"] == puuid and p["championName"].lower() == champion.lower():
                    matches.append(match_data)
                    break

        return matches

    async def _get_champion_matches_enhanced(
        self,
        session: aiohttp.ClientSession,
        puuid: str,
        champion: str,
        rank: str
    ) -> List[Dict]:
        """
        Get recent ranked matches with enhanced metadata for individual storage.
        Returns match data with additional fields for the game_records table.
        """
        url = f"{self.continent_url}/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&count=15"
        match_ids = await self._request(url, session)

        if not match_ids:
            return []

        matches = []

        for match_id in match_ids[:10]:  # Check up to 10 matches
            if len(matches) >= 5:  # Get up to 5 matches per player
                break

            match_url = f"{self.continent_url}/lol/match/v5/matches/{match_id}"
            match_data = await self._request(match_url, session)

            if not match_data:
                continue

            # Find the participant data for this player on this champion
            for p in match_data["info"]["participants"]:
                if p["puuid"] == puuid and p["championName"].lower() == champion.lower():
                    # Extract the game creation timestamp
                    game_creation_ms = match_data["info"].get("gameCreation", 0)
                    game_date = datetime.fromtimestamp(game_creation_ms / 1000) if game_creation_ms else None

                    # Detect role from position
                    team_position = p.get("teamPosition", "").upper()
                    role = self._position_to_role(team_position)

                    # Create enhanced match data with metadata
                    enhanced_match = {
                        "match_id": match_id,
                        "game_date": game_date.isoformat() if game_date else None,
                        "game_duration": match_data["info"].get("gameDuration", 0),
                        "rank": rank,
                        "win": p.get("win", False),
                        "role": role,
                        "participant": p,  # Full participant data for analysis
                        "raw_match": match_data  # Keep full match for legacy compatibility
                    }
                    matches.append(enhanced_match)
                    break

        return matches

    def _position_to_role(self, position: str) -> str:
        """Convert Riot's teamPosition to our role format"""
        position_map = {
            "TOP": "top",
            "JUNGLE": "jungle",
            "MIDDLE": "mid",
            "BOTTOM": "adc",
            "UTILITY": "support"
        }
        return position_map.get(position, "unknown")
