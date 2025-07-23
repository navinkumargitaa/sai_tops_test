"""
This module creates a SQLAlchemy engine for the SAI Badminton Database
and provides query functions to fetch ranking and tournament performance
data for singles and doubles badminton athletes.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

# -----------------------------#
# Database Connection Setup
# -----------------------------#

# Create SQLAlchemy engine
sai_db_engine = create_engine("mysql+pymysql://root:root@localhost:3306/sai_badminton_viz_final")
SessionLocal = sessionmaker(bind=sai_db_engine)

# -----------------------------#
# Constants
# -----------------------------#

SINGLES_ATHLETE_IDS = [
    83950, 68870, 73173, 69093, 59687, 74481, 58664,
    68322, 99042, 91807, 97168, 70595, 82572
]

RANKING_CATEGORIES = [6, 7]

FOCUS_DOUBLES_TEAMS = [
    (72435, 70500),  # Satwiksairaj & Chirag
    (71612, 59966),  # Treesa & Gayatri
    (69560, 98187),  # Hariharan & Ruban
    (57372, 94165)   # Dhruv & Tanisha
]

DOUBLES_CONDITIONS = " OR ".join(
    f"(b.athlete_1_id = {a1} AND b.athlete_2_id = {a2})"
    for a1, a2 in FOCUS_DOUBLES_TEAMS
)

# -----------------------------#
# Query Functions
# -----------------------------#

def read_singles_ranking_progression() -> str:
    """
    Generate SQL query to fetch singles ranking progression data
    for selected athletes and ranking categories.

    :return: SQL query string
    """
    athlete_ids_str = ", ".join(map(str, SINGLES_ATHLETE_IDS))
    ranking_ids_str = ", ".join(map(str, RANKING_CATEGORIES))

    query = f"""
        SELECT DISTINCT 
            a.athlete_id AS athlete_id,
            b.display_name AS athlete_name,
            a.date AS ranking_date,
            a.world_ranking AS world_ranking,
            a.world_tour_ranking AS world_tour_ranking,
            a.olympics AS olympic_rank,
            a.ranking_category_id AS ranking_id
        FROM sai_badminton_viz_final.badminton_ranking_graph_ind a
        JOIN sai_badminton_viz_final.badminton_athlete b
            ON a.athlete_id = b.athlete_id
        WHERE a.athlete_id IN ({athlete_ids_str})
          AND a.ranking_category_id IN ({ranking_ids_str})
    """
    return query


def read_doubles_ranking_progression() -> str:
    """
    Generate SQL query to fetch ranking progression data
    for selected doubles teams (fixed athlete ID pairs).

    :return: SQL query string
    """
    query = f"""
        SELECT DISTINCT
            a.date AS ranking_date,
            a.world_ranking,
            a.world_tour_ranking,
            a.olympics AS olympic_rank,
        
            b.athlete_1_id,
            a1.display_name AS athlete_1_name,
            b.athlete_2_id,
            a2.display_name AS athlete_2_name,
        
            CONCAT(b.athlete_1_id, '-', b.athlete_2_id) AS composite_team_id,
            CONCAT(a1.display_name, ' & ', a2.display_name) AS team_display_name
        
        FROM sai_badminton_viz_final.badminton_team b
        JOIN sai_badminton_viz_final.badminton_ranking_graph_team a 
            ON a.team_id = b.row_id
        LEFT JOIN sai_badminton_viz_final.badminton_athlete a1 
            ON b.athlete_1_id = a1.athlete_id
        LEFT JOIN sai_badminton_viz_final.badminton_athlete a2 
            ON b.athlete_2_id = a2.athlete_id
        WHERE {DOUBLES_CONDITIONS};
    """
    return query


def read_singles_tournament_finishes() -> pd.DataFrame:
    """
    Fetch tournament finish data for selected singles athletes.

    :return: DataFrame with tournament results
    """
    athlete_ids_str = ", ".join(map(str, SINGLES_ATHLETE_IDS))

    query = f"""
        SELECT DISTINCT 
            a.tournament_id AS tournament_id,
            b.name AS tournament_name,
            b.grade AS tournament_grade,
            b.date AS tournament_date,
            b.year AS tournament_year,
            a.athlete_id AS athlete_id,
            a.name AS category,
            a.position AS final_position
        FROM sai_badminton_viz_final.badminton_athlete_tournament_draw a
        INNER JOIN sai_badminton_viz_final.badminton_athlete_tournament b
            ON a.tournament_id = b.tournament_id 
            AND a.athlete_id = b.athlete_id
        WHERE a.athlete_id IN ({athlete_ids_str})
        ORDER BY tournament_date;
    """

    return pd.read_sql(query, sai_db_engine)
