"""
SAI Archery Data Module

This module provides functions to generate SQL queries for retrieving archery-related
athlete performance data from the SAI database. It includes ranking history, competition
ranking, qualification results, and elimination match results.

Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine

# SQLAlchemy engine for connecting to the local SAI database
sai_db_engine = create_engine(url="mysql+pymysql://root:root@localhost:3306/sai")

# List of athlete IDs used in all queries
ATHLETE_IDS = [8734, 5354, 42056, 22125, 39665, 39157, 17262, 43889,
               45025, 31020, 17287, 45024, 27737, 27738]

# Comma-separated string of athlete IDs for use in SQL queries
ID_LIST_STR = ", ".join(map(str, ATHLETE_IDS))


def read_archery_ranking():
    """
    Generate a SQL query to fetch ranking history for selected archery athletes.

    Returns:
        str: SQL query string.
    """
    query = f"""
    SELECT * FROM sai.archery_athlete_ranking_history
    WHERE athlete_id IN ({ID_LIST_STR});
    """
    return query


def read_archery_comp_ranking():
    """
    Generate a SQL query to fetch individual competition ranking
    for selected archery athletes.

    Returns:
        str: SQL query string including athlete rankings and competition metadata.
    """
    query = f"""
    SELECT
        a.competition_id AS comp_id,
        b.name AS comp_full_name,
        b.name_short AS comp_short_name,
        b.place AS comp_place,
        b.date_from AS comp_date,
        a.athlete_id AS athlete_id,
        a.name AS athlete_name,
        a.rank AS comp_rank
    FROM
        sai.archery_competition_ind_ranking a
    JOIN
        sai.archery_competition b
        ON a.competition_id = b.competition_id
    WHERE
        a.athlete_id IN ({ID_LIST_STR});
    """
    return query


def read_archery_qual_results():
    """
    Generate a SQL query to fetch qualification round results
    for selected archery athletes.

    Returns:
        str: SQL query string including scores, golds, X-nines, and competition metadata.
    """
    query = f"""
    SELECT
        a.competition_id AS competition_id,
        a.athlete_id AS athlete_id,
        a.name AS athlete_name,
        a.rank AS qual_rank,
        a.gold AS qual_gold,
        a.x_nine AS qual_x_nine,
        a.score AS qual_score,
        b.name_short AS comp_name,
        b.name AS comp_full_name,
        b.place AS comp_place,
        b.level AS comp_level,
        b.sub_level AS comp_sub_level,
        b.date_from AS comp_date
    FROM sai.archery_competition_ind_qual_result a
    JOIN sai.archery_competition b
        ON a.competition_id = b.competition_id
    WHERE a.athlete_id IN ({ID_LIST_STR});
    """
    return query


def read_archery_elem_results():
    """
    Generate a SQL query to fetch elimination match results
    for selected archery athletes.

    Returns:
        str: SQL query string including match details and outcomes.
    """
    query = f"""
    SELECT
        a.competition_id AS competition_id,
        a.athlete_id AS athlete_id,
        a.athlete_name AS athlete_name,
        a.qual_rank AS qual_rank,
        c.phase AS match_phase,
        a.arrow AS arrow,
        a.sp AS set_points,
        a.win_lose AS win_lose,
        a.bye AS bye,
        b.name_short AS comp_name,
        b.name AS comp_full_name,
        b.level AS comp_level,
        b.place AS comp_place,
        b.sub_level AS comp_sub_level,
        b.date_from AS comp_date
    FROM sai.archery_competition_individual_match_competitor a
    JOIN sai.archery_competition b
        ON a.competition_id = b.competition_id
    LEFT JOIN sai.archery_competition_individual_match c
        ON a.competition_ind_match_id = c.row_id
    WHERE a.athlete_id IN ({ID_LIST_STR});
    """
    return query
