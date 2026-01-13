#!/bin/bash
# Script to reset the database if corrupted

echo "Stopping containers..."
cd /home/milan/services/LeagueBuilds
docker compose down

echo "Removing PostgreSQL volume..."
docker volume rm leaguebuilds_postgres_data 2>/dev/null || true

echo "Restarting containers..."
docker compose up -d

echo "Waiting for database to be ready..."
sleep 10

echo "Checking logs..."
docker logs league_backend --tail 50

echo ""
echo "Database reset complete!"
echo "The tables will be recreated on startup."
echo ""
echo "Check API health: curl http://localhost:8000/health"
