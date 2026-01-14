#!/usr/bin/env python3
"""
Script pour régénérer manuellement la tier list.

Usage:
    python scripts/generate_tierlist.py
    python scripts/generate_tierlist.py --rank MASTER
    python scripts/generate_tierlist.py --rank GRANDMASTER
    python scripts/generate_tierlist.py --all
"""

import asyncio
import argparse
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tier_list_worker import tier_list_worker


async def main():
    parser = argparse.ArgumentParser(description="Régénérer la tier list manuellement")
    parser.add_argument(
        "--rank",
        type=str,
        default="MASTER",
        choices=["MASTER", "GRANDMASTER", "CHALLENGER"],
        help="Rank pour lequel générer la tier list (défaut: MASTER)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Générer la tier list pour tous les ranks"
    )

    args = parser.parse_args()

    ranks = ["MASTER", "GRANDMASTER", "CHALLENGER"] if args.all else [args.rank]

    for rank in ranks:
        print(f"Génération de la tier list pour {rank}...")
        try:
            tier_list = await tier_list_worker.generate_tier_list(rank)

            # Afficher le résumé
            counts = {tier: len(entries) for tier, entries in tier_list.items()}
            total = sum(counts.values())

            print(f"  ✓ Tier list générée: {total} entrées")
            print(f"    S: {counts['S']} | A: {counts['A']} | B: {counts['B']} | C: {counts['C']} | D: {counts['D']}")

        except Exception as e:
            print(f"  ✗ Erreur: {e}")
            return 1

    print("\nTerminé!")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
