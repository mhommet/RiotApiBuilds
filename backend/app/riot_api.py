import aiohttp
import asyncio
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Champion name to ID mapping (needed for mastery endpoint)
# Will be fetched from Data Dragon on init
CHAMPION_IDS: Dict[str, int] = {}


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

            url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
            async with session.get(url) as resp:
                data = await resp.json()
                for champ in data["data"].values():
                    self.champion_ids[champ["id"].lower()] = int(champ["key"])

            logger.info(f"Loaded {len(self.champion_ids)} champion IDs")
        except Exception as e:
            logger.error(f"Failed to load champion IDs: {e}")

    async def get_champion_leaderboard(
        self,
        champion: str,
        role: str,
        rank: str = "MASTER",
        limit: int = 5
    ) -> List[Dict]:
        """Get top players for a champion using mastery-based filtering"""
        logger.info(f"Fetching {champion} {role} players ({rank})...")

        async with aiohttp.ClientSession() as session:
            # Load champion IDs
            await self._ensure_champion_ids(session)
            champion_id = self.champion_ids.get(champion.lower())

            if not champion_id:
                logger.error(f"Unknown champion: {champion}")
                return []

            # Get high elo league
            if rank == "CHALLENGER":
                url = f"{self.base_url}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
            elif rank == "GRANDMASTER":
                url = f"{self.base_url}/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5"
            else:
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

            for i, player in enumerate(entries[:80]):  # Check top 80 by LP only
                if len(champion_players) >= limit:
                    break

                puuid = player.get('puuid')
                if not puuid:
                    continue

                # Quick mastery check - 1 request instead of 15+
                mastery_url = f"{self.base_url}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/by-champion/{champion_id}"
                mastery = await self._request(mastery_url, session)

                if not mastery or mastery.get("championPoints", 0) < 30000:
                    continue  # Skip if less than 30k mastery

                players_with_mastery += 1
                logger.info(f"Found player with {mastery.get('championPoints', 0):,} mastery on {champion}")

                # Now get their matches - worth the requests
                matches = await self._get_champion_matches(session, puuid, champion)

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
        """Get recent ranked matches for a champion"""
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
