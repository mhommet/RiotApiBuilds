"""
Configuration des poids pour l'agrégation des données de build.
"""

# Poids par rank (les joueurs de haut rang ont plus de poids)
RANK_WEIGHTS = {
    "CHALLENGER": 1.0,
    "GRANDMASTER": 0.9,
    "MASTER": 0.8,
    "DIAMOND": 0.7
}

# Tous les ranks à collecter (ordre de priorité pour la rotation)
ALL_RANKS = ["CHALLENGER", "GRANDMASTER", "MASTER", "DIAMOND"]

# Seuils de données
MIN_GAMES_THRESHOLD = 15      # Minimum de parties pour considérer les données fiables
IDEAL_GAMES_TARGET = 100      # Objectif idéal de parties pour un score de qualité maximal
MAX_GAMES_PER_RANK = 50       # Limite de parties stockées par rank pour éviter le biais

# Durée de validité des données
CACHE_DURATION_HOURS = 24     # Durée du cache des builds agrégés
REFRESH_INTERVAL_DAYS = 7     # Intervalle maximum sans nouvelle collecte

# Nombre de joueurs à interroger par rank
PLAYERS_PER_RANK = 25         # Augmenté pour collecter plus de données


def get_age_weight(days_old: int, is_previous_patch: bool = False) -> float:
    """
    Calcule le poids basé sur l'ancienneté de la partie.

    Args:
        days_old: Nombre de jours depuis la partie
        is_previous_patch: True si la partie est du patch précédent

    Returns:
        Poids entre 0.0 et 1.0
    """
    if is_previous_patch:
        return 0.5

    if days_old <= 7:
        return 1.0
    elif days_old <= 14:
        return 0.75
    else:
        return 0.5


def get_combined_weight(rank: str, days_old: int, is_previous_patch: bool = False) -> float:
    """
    Calcule le poids combiné (rank × ancienneté).

    Args:
        rank: Rank du joueur (CHALLENGER, GRANDMASTER, MASTER, DIAMOND)
        days_old: Nombre de jours depuis la partie
        is_previous_patch: True si la partie est du patch précédent

    Returns:
        Poids combiné
    """
    rank_weight = RANK_WEIGHTS.get(rank, 0.7)
    age_weight = get_age_weight(days_old, is_previous_patch)
    return rank_weight * age_weight


def compute_quality_score(
    total_games: int,
    games_by_rank: dict,
    avg_age_days: float
) -> float:
    """
    Calcule un score de qualité des données (0-100).

    Composition du score:
    - Quantité (50 points max): basé sur le nombre total de parties
    - Diversité des ranks (25 points max): basé sur le nombre de ranks représentés
    - Fraîcheur (25 points max): basé sur l'âge moyen des données

    Args:
        total_games: Nombre total de parties analysées
        games_by_rank: Dict avec le nombre de parties par rank
        avg_age_days: Âge moyen des parties en jours

    Returns:
        Score de qualité entre 0 et 100
    """
    # Score quantité (0-50 points)
    quantity_score = min(total_games / IDEAL_GAMES_TARGET * 50, 50)

    # Score diversité (0-25 points, 6.25 par rank représenté)
    ranks_represented = len([r for r in games_by_rank if games_by_rank.get(r, 0) > 0])
    diversity_score = ranks_represented * 6.25

    # Score fraîcheur (0-25 points, diminue avec l'âge)
    freshness_score = max(0, 25 - avg_age_days)

    return round(quantity_score + diversity_score + freshness_score, 1)
