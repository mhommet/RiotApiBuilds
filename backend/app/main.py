from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
import os

from app.database import init_db, AsyncSessionLocal
from app.riot_api import RiotAPI
from app.background_worker import worker
from app.tier_list_worker import tier_list_worker
from app.analyzer import analyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    # Startup
    logger.info("Starting League Builds API...")
    await init_db()

    # Launch background workers
    asyncio.create_task(worker.run())
    logger.info("Build generator worker started")

    asyncio.create_task(tier_list_worker.run())
    logger.info("Tier list worker started")

    logger.info("API Ready")

    yield

    # Shutdown
    worker.stop()
    tier_list_worker.stop()
    logger.info("Shutting down...")


app = FastAPI(
    title="League Builds API",
    description="API for optimal League of Legends builds based on high elo player analysis",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Riot API client
riot_api = RiotAPI(
    api_key=os.getenv("RIOT_API_KEY"),
    region=os.getenv("RIOT_REGION", "euw1"),
    continent=os.getenv("RIOT_CONTINENT", "europe")
)


@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "name": "League Builds API",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs",
        "endpoints": {
            "build": "/api/v1/build/{champion}/{role}",
            "tierlist": "/api/v1/tierlist",
            "worker_status": "/api/v1/stats/worker",
            "health": "/health"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint for Docker"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/v1/build/{champion}/{role}")
async def get_build(
    champion: str,
    role: str,
    rank: str = Query("MASTER", enum=["MASTER", "GRANDMASTER", "CHALLENGER"]),
    force_refresh: bool = Query(False, description="Force refresh from API")
):
    """
    Get optimal build for a champion/role combination.

    Returns cached data if available (< 24h), otherwise returns 404
    with a message to retry later.
    """
    champion_lower = champion.lower()
    role_lower = role.lower()

    logger.info(f"Fetching build: {champion_lower} {role_lower} {rank}")

    # Check cache in DB
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT data, timestamp, games_analyzed, winrate
                    FROM builds
                    WHERE champion = :champion AND role = :role AND rank = :rank
                    ORDER BY timestamp DESC
                    LIMIT 1
                """),
                {"champion": champion_lower, "role": role_lower, "rank": rank}
            )
            row = result.fetchone()

            if row and not force_refresh:
                build_data, timestamp, games, winrate = row
                age = datetime.utcnow() - timestamp

                if age < timedelta(hours=24):
                    logger.info(f"Cache hit: {champion_lower} {role_lower} (age: {age})")
                    return {
                        "champion": champion,
                        "role": role,
                        "rank": rank,
                        "build": build_data,
                        "cached": True,
                        "cache_age_hours": round(age.total_seconds() / 3600, 2),
                        "games_analyzed": games,
                        "winrate": winrate
                    }

    except Exception as e:
        logger.error(f"Database error: {e}")

    # No valid cache found
    raise HTTPException(
        status_code=404,
        detail={
            "message": f"Build for {champion} {role} not found in cache",
            "suggestion": "The build is being generated. Please try again in a few minutes.",
            "worker_status": worker.get_status()
        }
    )


@app.get("/api/v1/tierlist")
async def get_tierlist(
    rank: str = Query("MASTER", enum=["MASTER", "GRANDMASTER", "CHALLENGER"])
):
    """
    Get the current tier list.

    Returns champions grouped by tier (S, A, B, C, D) based on
    winrate and pickrate analysis.
    """
    logger.info(f"Fetching tier list for {rank}")

    try:
        tier_list = await tier_list_worker.get_tier_list(rank)

        # Count champions in each tier
        counts = {tier: len(champs) for tier, champs in tier_list.items()}
        total = sum(counts.values())

        return {
            "rank": rank,
            "tier_list": tier_list,
            "counts": counts,
            "total_champions": total,
            "last_update": tier_list_worker.last_generation.isoformat() if tier_list_worker.last_generation else None
        }

    except Exception as e:
        logger.error(f"Error fetching tier list: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": "Error fetching tier list", "error": str(e)}
        )


@app.get("/api/v1/stats/worker")
async def get_worker_stats():
    """
    Get detailed worker status.

    Returns information about build generation progress,
    tier list generation, and overall system health.
    """
    build_worker_status = worker.get_status()
    tier_worker_status = tier_list_worker.get_status()

    # Get database stats
    db_stats = {}
    try:
        async with AsyncSessionLocal() as db:
            # Count total builds
            result = await db.execute(text("SELECT COUNT(*) FROM builds"))
            db_stats["total_builds"] = result.scalar()

            # Count recent builds (< 24h)
            result = await db.execute(
                text("SELECT COUNT(*) FROM builds WHERE timestamp > NOW() - INTERVAL '24 hours'")
            )
            db_stats["recent_builds"] = result.scalar()

            # Count unique champions with builds
            result = await db.execute(
                text("SELECT COUNT(DISTINCT champion) FROM builds")
            )
            db_stats["unique_champions"] = result.scalar()

            # Count tier list entries
            result = await db.execute(text("SELECT COUNT(*) FROM tier_list"))
            db_stats["tier_list_entries"] = result.scalar()

    except Exception as e:
        logger.error(f"Error fetching DB stats: {e}")
        db_stats["error"] = str(e)

    return {
        "build_worker": build_worker_status,
        "tier_list_worker": tier_worker_status,
        "database": db_stats,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/v1/worker/status")
async def worker_status():
    """Legacy endpoint for worker status (redirects to /api/v1/stats/worker)"""
    return await get_worker_stats()


@app.get("/api/v1/champions")
async def list_champions():
    """
    List all champions with available builds.

    Returns the list of champions that have at least one build cached.
    """
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT
                        champion,
                        ARRAY_AGG(DISTINCT role) as roles,
                        MAX(timestamp) as last_update,
                        AVG(winrate) as avg_winrate
                    FROM builds
                    WHERE timestamp > NOW() - INTERVAL '48 hours'
                    GROUP BY champion
                    ORDER BY champion
                """)
            )
            rows = result.fetchall()

            champions = []
            for row in rows:
                champions.append({
                    "champion": row[0],
                    "roles": list(row[1]) if row[1] else [],
                    "last_update": row[2].isoformat() if row[2] else None,
                    "avg_winrate": round(float(row[3]), 2) if row[3] else None
                })

            return {
                "count": len(champions),
                "champions": champions
            }

    except Exception as e:
        logger.error(f"Error listing champions: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": "Error listing champions", "error": str(e)}
        )


@app.get("/api/v1/champion/{champion}")
async def get_champion_all_roles(
    champion: str,
    rank: str = Query("MASTER", enum=["MASTER", "GRANDMASTER", "CHALLENGER"])
):
    """
    Get builds for all roles for a specific champion.
    """
    champion_lower = champion.lower()

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT role, data, timestamp, games_analyzed, winrate
                    FROM builds
                    WHERE champion = :champion AND rank = :rank
                      AND timestamp > NOW() - INTERVAL '48 hours'
                    ORDER BY role
                """),
                {"champion": champion_lower, "rank": rank}
            )
            rows = result.fetchall()

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"No builds found for {champion}"
                )

            builds = {}
            for row in rows:
                role = row[0]
                builds[role] = {
                    "build": row[1],
                    "timestamp": row[2].isoformat() if row[2] else None,
                    "games_analyzed": row[3],
                    "winrate": row[4]
                }

            return {
                "champion": champion,
                "rank": rank,
                "roles": builds
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching champion builds: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": f"Error fetching builds for {champion}", "error": str(e)}
        )
