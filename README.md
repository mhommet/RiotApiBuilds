# League Builds API

REST API that provides optimal League of Legends builds by analyzing high elo players (Master+).

## Features

- Analyzes builds from Master, Grandmaster, and Challenger players
- Automatic build generation for all champion/role combinations
- Tier list generation based on winrate and pickrate
- 24-hour caching with automatic refresh
- Background workers for continuous data updates

## Tech Stack

- **Backend**: FastAPI (Python 3.11)
- **Database**: PostgreSQL 15
- **Containerization**: Docker & Docker Compose
- **External API**: Riot Games API

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/build/{champion}/{role}` | Get optimal build for a champion/role |
| `GET /api/v1/tierlist` | Get current tier list |
| `GET /api/v1/champions` | List all champions with available builds |
| `GET /api/v1/champion/{champion}` | Get builds for all roles for a champion |
| `GET /api/v1/stats/worker` | Get worker status and statistics |
| `GET /health` | Health check endpoint |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Riot API Key ([Get one here](https://developer.riotgames.com/))

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/LeagueBuilds.git
cd LeagueBuilds
```

2. Create your environment file:
```bash
cp .env.example .env
```

3. Edit `.env` and add your Riot API key and set a secure database password.

4. Start the services:
```bash
docker compose up -d
```

5. The API will be available at `http://localhost:8000`

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_USER` | PostgreSQL username | `league_user` |
| `DB_PASSWORD` | PostgreSQL password | - |
| `DB_NAME` | Database name | `league_builds` |
| `RIOT_API_KEY` | Your Riot Games API key | - |
| `ENVIRONMENT` | Environment mode | `production` |
| `CACHE_DURATION_HOURS` | Build cache duration | `24` |
| `MAX_PLAYERS_ANALYZED` | Max players to analyze per build | `50` |

## Usage Examples

Get build for Aatrox Top:
```bash
curl http://localhost:8000/api/v1/build/aatrox/top
```

Get tier list for Master+ players:
```bash
curl http://localhost:8000/api/v1/tierlist?rank=MASTER
```

## License

MIT
