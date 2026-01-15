from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://league:leaguepass@db:5432/leaguebuilds")

# Assure-toi que l'URL utilise asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Cache pour le patch actuel
_current_patch_cache: Optional[str] = None
_previous_patch_cache: Optional[str] = None


async def get_db():
    """Dependency pour récupérer une session DB"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize database tables"""
    async with engine.begin() as conn:
        # Create builds table (legacy + new columns)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS builds (
                id SERIAL PRIMARY KEY,
                champion VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL,
                rank VARCHAR(20) NOT NULL,
                data JSONB NOT NULL,
                games_analyzed INTEGER,
                winrate FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Nouvelles colonnes pour l'agrégation multi-rang
                total_games_analyzed INTEGER,
                games_by_rank JSONB,
                recent_games INTEGER,
                oldest_game_date TIMESTAMP,
                current_patch VARCHAR(10),
                data_quality_score FLOAT,
                weighted_winrate FLOAT,
                weighted_pickrate FLOAT
            )
        """))

        # Add new columns to existing builds table (if they don't exist)
        new_columns = [
            ("total_games_analyzed", "INTEGER"),
            ("games_by_rank", "JSONB"),
            ("recent_games", "INTEGER"),
            ("oldest_game_date", "TIMESTAMP"),
            ("current_patch", "VARCHAR(10)"),
            ("data_quality_score", "FLOAT"),
            ("weighted_winrate", "FLOAT"),
            ("weighted_pickrate", "FLOAT"),
        ]
        for col_name, col_type in new_columns:
            try:
                await conn.execute(text(f"""
                    ALTER TABLE builds ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """))
            except Exception:
                pass  # Column might already exist

        # Create game_records table for individual games
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_records (
                id SERIAL PRIMARY KEY,
                match_id VARCHAR(50) UNIQUE NOT NULL,
                champion VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL,
                rank VARCHAR(20) NOT NULL,
                patch VARCHAR(10) NOT NULL,
                game_date TIMESTAMP NOT NULL,
                win BOOLEAN NOT NULL,
                game_duration INTEGER,
                runes JSONB,
                items JSONB,
                summoner_spells JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create patch_info table for tracking patches
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS patch_info (
                id SERIAL PRIMARY KEY,
                patch_version VARCHAR(10) UNIQUE NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_current BOOLEAN DEFAULT FALSE
            )
        """))

        # Create tier_list table (with role support for multi-role champions)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tier_list (
                id SERIAL PRIMARY KEY,
                champion VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL,
                tier VARCHAR(2) NOT NULL,
                rank VARCHAR(20) NOT NULL,
                winrate FLOAT,
                pickrate FLOAT,
                games_analyzed INTEGER,
                performance_score FLOAT,
                rank_in_role INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Add role column to existing tier_list table if it doesn't exist
        try:
            await conn.execute(text("""
                ALTER TABLE tier_list ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT 'unknown'
            """))
        except Exception:
            pass  # Column might already exist

        # Add rank_in_role column to existing tier_list table if it doesn't exist
        try:
            await conn.execute(text("""
                ALTER TABLE tier_list ADD COLUMN IF NOT EXISTS rank_in_role INTEGER
            """))
        except Exception:
            pass  # Column might already exist

        # Create worker_stats table for tracking progress
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS worker_stats (
                id SERIAL PRIMARY KEY,
                worker_type VARCHAR(50) NOT NULL,
                builds_generated INTEGER DEFAULT 0,
                builds_total INTEGER DEFAULT 0,
                last_champion VARCHAR(50),
                last_role VARCHAR(20),
                cycle_number INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'idle'
            )
        """))

        # Index pour optimiser les requêtes builds
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_builds_champion_role
            ON builds(champion, role, rank, timestamp DESC)
        """))

        # Index pour game_records
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_game_records_champion_role
            ON game_records(champion, role)
        """))

        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_game_records_patch
            ON game_records(patch, game_date DESC)
        """))

        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_game_records_match_id
            ON game_records(match_id)
        """))

        # Index pour tier_list
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_rank_timestamp
            ON tier_list(rank, timestamp DESC)
        """))

        # Index pour tier_list par champion et role
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_champion
            ON tier_list(champion, rank)
        """))

        # Index pour tier_list par role (pour filtrage par lane)
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_role
            ON tier_list(role, rank, performance_score DESC)
        """))

        # Index pour tier_list champion + role (pour requêtes multi-rôles)
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_champion_role
            ON tier_list(champion, role, rank)
        """))

    logger.info("Database tables initialized")


async def get_current_patch() -> Optional[str]:
    """Get the current patch version from database"""
    global _current_patch_cache
    if _current_patch_cache:
        return _current_patch_cache

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT patch_version FROM patch_info WHERE is_current = TRUE LIMIT 1")
        )
        row = result.fetchone()
        if row:
            _current_patch_cache = row[0]
            return _current_patch_cache
    return None


async def get_previous_patch() -> Optional[str]:
    """Get the previous patch version from database"""
    global _previous_patch_cache
    if _previous_patch_cache:
        return _previous_patch_cache

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("""
                SELECT patch_version FROM patch_info
                WHERE is_current = FALSE
                ORDER BY detected_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row:
            _previous_patch_cache = row[0]
            return _previous_patch_cache
    return None


async def update_patch_info(new_patch: str) -> bool:
    """
    Update patch info in database.
    Returns True if a new patch was detected.
    """
    global _current_patch_cache, _previous_patch_cache

    async with AsyncSessionLocal() as db:
        # Check if this patch already exists
        result = await db.execute(
            text("SELECT is_current FROM patch_info WHERE patch_version = :patch"),
            {"patch": new_patch}
        )
        row = result.fetchone()

        if row:
            # Patch exists, check if it's already current
            if row[0]:
                return False  # Already current, no change
            # Mark all as not current, then mark this one as current
            await db.execute(text("UPDATE patch_info SET is_current = FALSE"))
            await db.execute(
                text("UPDATE patch_info SET is_current = TRUE WHERE patch_version = :patch"),
                {"patch": new_patch}
            )
        else:
            # New patch detected
            await db.execute(text("UPDATE patch_info SET is_current = FALSE"))
            await db.execute(
                text("""
                    INSERT INTO patch_info (patch_version, is_current)
                    VALUES (:patch, TRUE)
                """),
                {"patch": new_patch}
            )

        await db.commit()

        # Invalidate caches
        _current_patch_cache = new_patch
        _previous_patch_cache = None

        logger.info(f"Patch updated to {new_patch}")
        return True
