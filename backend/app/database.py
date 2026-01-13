from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://league:leaguepass@db:5432/leaguebuilds")

# Assure-toi que l'URL utilise asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


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
        # Create builds table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS builds (
                id SERIAL PRIMARY KEY,
                champion VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL,
                rank VARCHAR(20) NOT NULL,
                data JSONB NOT NULL,
                games_analyzed INTEGER,
                winrate FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create tier_list table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tier_list (
                id SERIAL PRIMARY KEY,
                champion VARCHAR(50) NOT NULL,
                tier VARCHAR(2) NOT NULL,
                rank VARCHAR(20) NOT NULL,
                winrate FLOAT,
                pickrate FLOAT,
                games_analyzed INTEGER,
                performance_score FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

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

        # Index pour tier_list
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_rank_timestamp
            ON tier_list(rank, timestamp DESC)
        """))

        # Index pour tier_list par champion
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tier_list_champion
            ON tier_list(champion, rank)
        """))

    logger.info("Database tables initialized")
