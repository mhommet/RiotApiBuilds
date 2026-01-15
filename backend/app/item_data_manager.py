"""
Item Data Manager - Fetches items from Data Dragon and calculates gold efficiency

Data Dragon is Riot's official static data API (no API key required).
"""

import aiohttp
import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class ItemDataManager:
    """Gestionnaire des données d'items depuis Data Dragon"""

    VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
    ITEMS_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json"
    ITEM_IMAGE_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/img/item/{item_id}.png"

    CACHE_DIR = "data"
    CACHE_FILE = "data/items_cache.json"
    CACHE_DURATION = timedelta(hours=24)

    # Gold values for stat efficiency calculation (patch 16.1+)
    # Calculated from base items (Long Sword, Amplifying Tome, etc.)
    GOLD_VALUES = {
        # Main stats
        'FlatPhysicalDamageMod': 35,        # 1 AD = 35 gold (Long Sword: 10 AD = 350g)
        'FlatMagicDamageMod': 21.75,        # 1 AP = 21.75 gold (Amplifying Tome: 20 AP = 435g)
        'FlatArmorMod': 20,                 # 1 Armor = 20 gold (Cloth Armor: 15 Armor = 300g)
        'FlatSpellBlockMod': 18,            # 1 MR = 18 gold (Null-Magic Mantle: 25 MR = 450g)
        'FlatHPPoolMod': 2.67,              # 1 HP = 2.67 gold (Ruby Crystal: 150 HP = 400g)
        'FlatMPPoolMod': 1.4,               # 1 Mana = 1.4 gold (Sapphire Crystal: 250 Mana = 350g)

        # Percentage stats (Data Dragon returns decimals: 0.12 = 12%)
        'PercentAttackSpeedMod': 2500,      # 0.12 AS = 12% -> 0.12 * 2500 = 300g
        'FlatCritChanceMod': 4000,          # 0.20 = 20% -> 0.20 * 4000 = 800g
        'PercentMovementSpeedMod': 3950,    # 0.05 = 5% -> 0.05 * 3950 = 197.5g
        'PercentLifeStealMod': 2750,        # 0.10 = 10% -> 0.10 * 2750 = 275g
        'PercentOmnivampMod': 3000,         # Omnivamp in decimal
        'PercentArmorPenetrationMod': 3000, # Armor Pen% in decimal

        # Flat stats
        'FlatMovementSpeedMod': 12,         # 1 Flat MS = 12 gold
        'FlatHPRegenMod': 36,               # 1 HP regen/5 = 36 gold
        'FlatMPRegenMod': 50,               # 1 Mana regen/5 = 50 gold

        # Penetration
        'FlatPhysicalPenMod': 5,            # 1 Lethality = 5 gold
        'FlatMagicPenMod': 31.11,           # 1 Magic Pen = 31.11 gold

        # Ability Haste (CDR removed since S11)
        'AbilityHasteMod': 26.67,           # 1 AH = 26.67 gold
    }

    def __init__(self):
        self.items: Optional[Dict] = None
        self.version: Optional[str] = None
        self._last_fetch: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def get_items(self, force_refresh: bool = False) -> Dict:
        """
        Get all items with gold efficiency calculated.

        Uses cache if valid (< 24h), otherwise fetches from Data Dragon.
        """
        async with self._lock:
            # Check memory cache first
            if not force_refresh and self.items and self._is_memory_cache_valid():
                logger.debug("Using memory cache for items")
                return self.items

            # Check file cache
            if not force_refresh and self._is_file_cache_valid():
                logger.info("Loading items from file cache")
                return await self._load_cache()

            # Fetch from Data Dragon
            logger.info("Fetching items from Data Dragon...")
            return await self._fetch_from_ddragon()

    async def get_item(self, item_id: str) -> Optional[Dict]:
        """Get a specific item by ID"""
        items = await self.get_items()
        return items.get('data', {}).get(item_id)

    async def get_version(self) -> str:
        """Get the current patch version"""
        if not self.version:
            await self.get_items()
        return self.version

    async def _fetch_from_ddragon(self) -> Dict:
        """Fetch items from Data Dragon API"""
        async with aiohttp.ClientSession() as session:
            try:
                # 1. Get latest version
                async with session.get(
                    self.VERSIONS_URL,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch versions: HTTP {resp.status}")
                        raise Exception(f"Failed to fetch versions: HTTP {resp.status}")
                    versions = await resp.json()
                    self.version = versions[0]  # e.g., "16.1.1"
                    logger.info(f"Current Data Dragon version: {self.version}")

                # 2. Fetch items
                items_url = self.ITEMS_URL.format(version=self.version)
                async with session.get(
                    items_url,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch items: HTTP {resp.status}")
                        raise Exception(f"Failed to fetch items: HTTP {resp.status}")
                    items_data = await resp.json()

                # 3. Calculate gold efficiency for each item
                processed_count = 0
                for item_id, item in items_data['data'].items():
                    efficiency = self._calculate_gold_efficiency(item)
                    item['gold_efficiency'] = efficiency
                    item['image_url'] = self.ITEM_IMAGE_URL.format(
                        version=self.version,
                        item_id=item_id
                    )
                    if efficiency is not None:
                        processed_count += 1

                logger.info(f"Processed {processed_count} items with gold efficiency")

                # 4. Save to memory and file cache
                self.items = items_data
                self._last_fetch = datetime.now()
                await self._save_cache(items_data)

                return items_data

            except aiohttp.ClientError as e:
                logger.error(f"Network error fetching from Data Dragon: {e}")
                # Try to return cached data if available
                if self._is_file_cache_valid():
                    logger.warning("Using stale cache due to network error")
                    return await self._load_cache()
                raise
            except Exception as e:
                logger.error(f"Error fetching items from Data Dragon: {e}")
                raise

    def _calculate_gold_efficiency(self, item: Dict) -> Optional[float]:
        """
        Calculate the gold efficiency of an item.

        Gold efficiency = (total_gold_value_of_stats / item_price) * 100

        Returns None for non-purchasable items or items with no price.
        Returns 0.0 for items with no calculable stats.
        """
        # Check if item is purchasable
        gold_info = item.get('gold', {})
        if not gold_info.get('purchasable', False):
            return None

        price = gold_info.get('total', 0)
        if price == 0:
            return None

        # Calculate total gold value of stats
        total_gold_value = 0.0
        stats = item.get('stats', {})

        for stat_name, stat_value in stats.items():
            if stat_name in self.GOLD_VALUES:
                gold_value = stat_value * self.GOLD_VALUES[stat_name]
                total_gold_value += gold_value

        # If no calculable stats, return 0
        if total_gold_value == 0:
            return 0.0

        # Calculate efficiency percentage
        efficiency = (total_gold_value / price) * 100
        return round(efficiency, 1)

    def _is_memory_cache_valid(self) -> bool:
        """Check if memory cache is still valid"""
        if not self._last_fetch:
            return False
        return datetime.now() - self._last_fetch < self.CACHE_DURATION

    def _is_file_cache_valid(self) -> bool:
        """Check if file cache exists and is still valid"""
        try:
            if not os.path.exists(self.CACHE_FILE):
                return False

            with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                cache_time = datetime.fromisoformat(cache['timestamp'])
                return datetime.now() - cache_time < self.CACHE_DURATION
        except Exception as e:
            logger.warning(f"Error checking cache validity: {e}")
            return False

    async def _load_cache(self) -> Dict:
        """Load items from file cache"""
        try:
            with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                self.version = cache['version']
                self.items = cache['data']
                self._last_fetch = datetime.fromisoformat(cache['timestamp'])
                logger.info(f"Loaded {len(self.items.get('data', {}))} items from cache (version {self.version})")
                return self.items
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            raise

    async def _save_cache(self, data: Dict):
        """Save items to file cache"""
        try:
            # Ensure cache directory exists
            os.makedirs(self.CACHE_DIR, exist_ok=True)

            cache = {
                'timestamp': datetime.now().isoformat(),
                'version': self.version,
                'data': data
            }

            with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved items cache to {self.CACHE_FILE}")
        except Exception as e:
            logger.error(f"Error saving cache: {e}")

    async def get_items_by_tag(self, tag: str) -> List[Dict]:
        """
        Get all items with a specific tag (e.g., "Boots", "Damage", "Health").
        """
        items = await self.get_items()
        matching = []

        for item_id, item in items.get('data', {}).items():
            if tag in item.get('tags', []):
                item_copy = item.copy()
                item_copy['id'] = item_id
                matching.append(item_copy)

        return matching

    async def get_purchasable_items(self, min_efficiency: Optional[float] = None) -> List[Dict]:
        """
        Get all purchasable items, optionally filtered by minimum gold efficiency.
        """
        items = await self.get_items()
        purchasable = []

        for item_id, item in items.get('data', {}).items():
            if not item.get('gold', {}).get('purchasable', False):
                continue

            efficiency = item.get('gold_efficiency')

            if min_efficiency is not None and efficiency is not None:
                if efficiency < min_efficiency:
                    continue

            item_copy = item.copy()
            item_copy['id'] = item_id
            purchasable.append(item_copy)

        # Sort by gold efficiency descending
        purchasable.sort(
            key=lambda x: x.get('gold_efficiency') or 0,
            reverse=True
        )

        return purchasable


# Global instance for use across the application
item_manager = ItemDataManager()
