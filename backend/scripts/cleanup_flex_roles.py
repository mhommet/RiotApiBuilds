#!/usr/bin/env python3
"""
Script pour supprimer les entrées avec des rôles invalides (flex, unknown, etc.)

Usage:
    python scripts/cleanup_flex_roles.py
    python scripts/cleanup_flex_roles.py --dry-run  # Voir ce qui serait supprimé sans supprimer
"""

import asyncio
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database import AsyncSessionLocal


async def main():
    parser = argparse.ArgumentParser(description="Supprimer les entrées avec des rôles invalides")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Afficher ce qui serait supprimé sans supprimer"
    )
    args = parser.parse_args()

    async with AsyncSessionLocal() as db:
        tables = ["tier_list", "builds", "game_records"]

        for table in tables:
            # Compter les entrées avec des rôles invalides
            result = await db.execute(
                text(f"SELECT role, COUNT(*) FROM {table} WHERE role NOT IN ('top', 'jungle', 'mid', 'adc', 'support') GROUP BY role")
            )
            rows = result.fetchall()

            if rows:
                total = sum(row[1] for row in rows)
                print(f"Table '{table}': {total} entrées avec rôles invalides")
                for role, count in rows:
                    print(f"  - '{role}': {count}")

                if not args.dry_run:
                    await db.execute(
                        text(f"DELETE FROM {table} WHERE role NOT IN ('top', 'jungle', 'mid', 'adc', 'support')")
                    )
                    print(f"  ✓ Supprimé {total} entrées")
            else:
                print(f"Table '{table}': OK (aucun rôle invalide)")

        if not args.dry_run:
            await db.commit()
            print("\n✓ Nettoyage terminé!")
        else:
            print("\n[DRY-RUN] Aucune modification effectuée")


if __name__ == "__main__":
    asyncio.run(main())
