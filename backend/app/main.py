from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
import os

from app.database import (
    init_db,
    AsyncSessionLocal,
    get_current_patch,
    get_previous_patch
)
from app.riot_api import RiotAPI
from app.background_worker import worker
from app.tier_list_worker import tier_list_worker
from app.analyzer import analyzer
from app.weights import MIN_GAMES_THRESHOLD

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
    logger.info("Starting FocusAPI...")
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
    title="FocusAPI",
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
        "name": "FocusAPI",
        "version": "1.1.0",
        "status": "operational",
        "docs": "/docs",
        "endpoints": {
            "build": "/api/v1/build/{champion}/{role}",
            "builds_all_roles": "/api/v1/builds/{champion}",
            "champion_all_roles": "/api/v1/champion/{champion}",
            "tierlist": "/api/v1/tierlist?role=mid",
            "tierlist_flat": "/api/v1/tierlist/flat?role=mid",
            "champions": "/api/v1/champions",
            "worker_status": "/api/v1/stats/worker",
            "health": "/health"
        },
        "features": {
            "multi_role_support": True,
            "role_filtering": ["top", "jungle", "mid", "adc", "support"]
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
    force_refresh: bool = Query(False, description="Force collection and refresh from API")
):
    """
    Get optimal build for a champion/role combination.

    Automatically aggregates data from Diamond+ ranks (Diamond, Master,
    Grandmaster, Challenger) with weighted statistics.

    Returns cached aggregated data if available (< 24h), otherwise
    triggers collection and returns 404 with status.
    """
    champion_lower = champion.lower()
    role_lower = role.lower()

    logger.info(f"Fetching aggregated build: {champion_lower} {role_lower}")

    # Force refresh: trigger collection
    if force_refresh:
        logger.info(f"Force refresh requested for {champion_lower} {role_lower}")
        asyncio.create_task(worker.collect_games_for_champion(champion_lower, role_lower))

    # Check cache in DB (look for AGGREGATED rank which is the new format)
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT
                        data, timestamp, games_analyzed, winrate,
                        total_games_analyzed, games_by_rank, recent_games,
                        oldest_game_date, current_patch, data_quality_score,
                        weighted_winrate, weighted_pickrate
                    FROM builds
                    WHERE champion = :champion AND role = :role
                      AND (rank = 'AGGREGATED' OR rank = 'MASTER')
                    ORDER BY
                        CASE WHEN rank = 'AGGREGATED' THEN 0 ELSE 1 END,
                        timestamp DESC
                    LIMIT 1
                """),
                {"champion": champion_lower, "role": role_lower}
            )
            row = result.fetchone()

            if row and not force_refresh:
                (build_data, timestamp, games, winrate,
                 total_games, games_by_rank, recent_games,
                 oldest_game, current_patch, quality_score,
                 weighted_winrate, weighted_pickrate) = row

                age = datetime.utcnow() - timestamp

                if age < timedelta(hours=24):
                    logger.info(f"Cache hit: {champion_lower} {role_lower} (age: {age})")

                    response = {
                        "champion": champion,
                        "role": role,
                        "build": build_data,
                        "cached": True,
                        "cache_age_hours": round(age.total_seconds() / 3600, 2),
                        # Legacy fields
                        "games_analyzed": games or total_games or 0,
                        "winrate": winrate,
                        # New metrics
                        "total_games_analyzed": total_games or games or 0,
                        "games_by_rank": games_by_rank or {"CHALLENGER": 0, "GRANDMASTER": 0, "MASTER": 0, "DIAMOND": 0},
                        "recent_games": recent_games or 0,
                        "oldest_game_date": oldest_game.isoformat() if oldest_game else None,
                        "current_patch": current_patch,
                        "data_quality_score": quality_score or 0,
                        "weighted_winrate": weighted_winrate or winrate or 0,
                        "weighted_pickrate": weighted_pickrate or 0
                    }

                    return response

    except Exception as e:
        logger.error(f"Database error: {e}")

    # Check if we have any games at all
    game_count = 0
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT COUNT(*) FROM game_records
                    WHERE champion = :champion AND role = :role
                """),
                {"champion": champion_lower, "role": role_lower}
            )
            game_count = result.scalar() or 0
    except Exception:
        pass

    # No valid cache found
    raise HTTPException(
        status_code=404,
        detail={
            "message": f"Build for {champion} {role} not found in cache",
            "suggestion": "The build is being generated. Please try again in a few minutes.",
            "games_in_queue": game_count,
            "min_games_required": MIN_GAMES_THRESHOLD,
            "worker_status": worker.get_status()
        }
    )


@app.get("/api/v1/tierlist")
async def get_tierlist(
    role: Optional[str] = Query(None, enum=["top", "jungle", "mid", "adc", "support"])
):
    """
    Get the current tier list.

    Returns champions grouped by tier (S, A, B, C, D) based on
    winrate and pickrate analysis from Diamond+ players (aggregated data).

    Each champion can appear MULTIPLE TIMES (once per role) with different stats.
    For example: Akali mid (S tier, 54% WR) and Akali top (B tier, 48% WR).

    Args:
        role: Optional role filter (top, jungle, mid, adc, support)
              If not specified, returns all roles
    """
    logger.info(f"Fetching tier list" + (f" role={role}" if role else ""))

    try:
        tier_list = await tier_list_worker.get_tier_list(role)

        # Count entries in each tier (now champion+role combos, not just champions)
        counts = {tier: len(entries) for tier, entries in tier_list.items()}
        total = sum(counts.values())

        response = {
            "tier_list": tier_list,
            "counts": counts,
            "total_entries": total,
            "last_update": tier_list_worker.last_generation.isoformat() if tier_list_worker.last_generation else None
        }

        if role:
            response["role_filter"] = role

        return response

    except Exception as e:
        logger.error(f"Error fetching tier list: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": "Error fetching tier list", "error": str(e)}
        )


@app.get("/api/v1/tierlist/flat")
async def get_tierlist_flat(
    role: Optional[str] = Query(None, enum=["top", "jungle", "mid", "adc", "support"])
):
    """
    Get the tier list as a flat array (useful for frontend display).

    Returns a flat list of champion+role entries sorted by performance score.
    Each entry includes: champion, role, tier, winrate, pickrate, games_analyzed, performance_score.

    Args:
        role: Optional role filter (top, jungle, mid, adc, support)
    """
    logger.info(f"Fetching flat tier list" + (f" role={role}" if role else ""))

    try:
        tier_list = await tier_list_worker.get_tier_list(role)

        # Flatten the tier list into a single array sorted by performance_score
        flat_list = []
        for tier, entries in tier_list.items():
            for entry in entries:
                flat_list.append(entry)

        # Sort by performance_score descending
        flat_list.sort(key=lambda x: x["performance_score"], reverse=True)

        return {
            "role_filter": role,
            "entries": flat_list,
            "total_entries": len(flat_list),
            "last_update": tier_list_worker.last_generation.isoformat() if tier_list_worker.last_generation else None
        }

    except Exception as e:
        logger.error(f"Error fetching flat tier list: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": "Error fetching tier list", "error": str(e)}
        )


@app.get("/api/v1/stats/worker")
async def get_worker_stats():
    """
    Get detailed worker status.

    Returns information about build generation progress,
    tier list generation, game collection stats, and overall system health.
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

            # Game records stats (new)
            result = await db.execute(text("SELECT COUNT(*) FROM game_records"))
            db_stats["total_games_stored"] = result.scalar()

            # Games by rank
            result = await db.execute(
                text("""
                    SELECT rank, COUNT(*) as count
                    FROM game_records
                    GROUP BY rank
                """)
            )
            db_stats["games_by_rank"] = {row[0]: row[1] for row in result.fetchall()}

            # Recent games (7 days)
            result = await db.execute(
                text("SELECT COUNT(*) FROM game_records WHERE game_date > NOW() - INTERVAL '7 days'")
            )
            db_stats["recent_games_7d"] = result.scalar()

            # Patch info
            result = await db.execute(
                text("SELECT patch_version FROM patch_info WHERE is_current = TRUE LIMIT 1")
            )
            row = result.fetchone()
            db_stats["current_patch"] = row[0] if row else None

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


@app.get("/api/v1/builds/{champion}")
async def get_builds_all_roles(champion: str):
    """
    Get all builds for a champion across all roles.

    Returns builds for each role the champion is played in, with tier information
    and the primary_role (most played role).

    Format:
    {
        "champion": "akali",
        "primary_role": "mid",
        "roles": [
            {"role": "mid", "tier": "A", "winrate": 52.3, ...},
            {"role": "top", "tier": "B", "winrate": 48.7, ...}
        ]
    }
    """
    champion_lower = champion.lower()

    try:
        async with AsyncSessionLocal() as db:
            # Get builds with tier info from tier_list
            result = await db.execute(
                text("""
                    SELECT DISTINCT ON (b.role)
                        b.role, b.data, b.timestamp, b.games_analyzed, b.winrate,
                        b.total_games_analyzed, b.games_by_rank, b.data_quality_score,
                        b.weighted_winrate, b.current_patch,
                        t.tier, t.pickrate, t.performance_score
                    FROM builds b
                    LEFT JOIN tier_list t ON b.champion = t.champion AND b.role = t.role
                    WHERE b.champion = :champion
                      AND b.timestamp > NOW() - INTERVAL '48 hours'
                    ORDER BY b.role,
                        CASE WHEN b.rank = 'AGGREGATED' THEN 0 ELSE 1 END,
                        b.timestamp DESC
                """),
                {"champion": champion_lower}
            )
            rows = result.fetchall()

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"No builds found for {champion}"
                )

            roles_data = []
            max_games = 0
            primary_role = None

            for row in rows:
                role = row[0]
                games = row[3] or row[5] or 0

                # Track primary role (most games)
                if games > max_games:
                    max_games = games
                    primary_role = role

                roles_data.append({
                    "role": role,
                    "tier": row[10] or "?",  # tier from tier_list
                    "winrate": round(float(row[4]), 2) if row[4] else 0,
                    "pickrate": round(float(row[11]), 2) if row[11] else 0,
                    "games_analyzed": games,
                    "performance_score": round(float(row[12]), 2) if row[12] else 0,
                    "build": row[1],  # Full build data
                    "data_quality_score": row[7] or 0,
                    "current_patch": row[9]
                })

            # Sort by games_analyzed (primary role first)
            roles_data.sort(key=lambda x: x["games_analyzed"], reverse=True)

            return {
                "champion": champion,
                "primary_role": primary_role,
                "roles_count": len(roles_data),
                "roles": roles_data
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching builds for {champion}: {e}")
        raise HTTPException(
            status_code=500,
            detail={"message": f"Error fetching builds for {champion}", "error": str(e)}
        )


@app.get("/api/v1/builds/{champion}/{role}")
async def get_build_for_role(champion: str, role: str):
    """
    Get build for a specific champion and role.
    Alias for /api/v1/build/{champion}/{role} for consistency.
    """
    return await get_build(champion, role)


@app.get("/api/v1/champion/{champion}")
async def get_champion_all_roles(champion: str):
    """
    Get builds for all roles for a specific champion.
    Uses aggregated Diamond+ data.

    DEPRECATED: Use /api/v1/builds/{champion} instead for tier and primary_role info.
    """
    champion_lower = champion.lower()

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("""
                    SELECT DISTINCT ON (role)
                        role, data, timestamp, games_analyzed, winrate,
                        total_games_analyzed, games_by_rank, data_quality_score,
                        weighted_winrate, current_patch
                    FROM builds
                    WHERE champion = :champion
                      AND timestamp > NOW() - INTERVAL '48 hours'
                    ORDER BY role,
                        CASE WHEN rank = 'AGGREGATED' THEN 0 ELSE 1 END,
                        timestamp DESC
                """),
                {"champion": champion_lower}
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
                    "winrate": row[4],
                    "total_games_analyzed": row[5] or row[3],
                    "games_by_rank": row[6] or {},
                    "data_quality_score": row[7] or 0,
                    "weighted_winrate": row[8] or row[4],
                    "current_patch": row[9]
                }

            return {
                "champion": champion,
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
