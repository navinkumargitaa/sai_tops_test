"""
SAI Shooting Data Module

This module provides functions to generate SQL queries for retrieving shooting-related
athlete performance data from the SAI database.
Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine

# SQLAlchemy engine for connecting to the local SAI database
sai_db_engine = create_engine("mysql+pymysql://root:root@localhost:3306/sai_shooting_final")

# List of top priority athletes (used in filters)
TOP_ATHLETES = [
    'TOMAR Aishwary Pratap Singh', 'SHEORAN Akhil', 'NARUKA Anant Jeet Singh', 'BABUTA Arjun',
    'PANWAR Divyansh Singh', 'VALARIVAN Elavenil', 'SINGH Esha', 'CHAUHAN Maheshwari',
    'BHAKER Manu', 'GHOSH Mehuli', 'NANCY Nancy', 'PALAK Palak', 'RAMITA Ramita',
    'PATIL Rudrankksh Balasaheb', 'SINGH Sarabjot', 'NARWAL Shiva', 'SAMRA Sift Kaur',
    'KUSALE Swapnil', 'ANISH Anish', 'MOUDGIL Anjum', 'CHEEMA Arjun Singh', 'CHOUKSEY Ashi',
    'RATHORE Darshna', 'THADIGOL SUBBARAJU Divya', 'KHANGURA Gurjoat', 'HAZARIKA Hriday',
    'CHENAI Kynan', 'TONDAIMAN Prithviraj', 'DHILLON Raiza', 'KUMARI Rajeshwari',
    'SANGWAN Rhythm', 'SINGH Sandeep', 'SINGH Shreyasi', 'RAO Surbhi', 'SEN Tilottama',
    'TOMAR Varun', 'SIDHU Vijayveer'
]


def read_shooting_results() -> str:
    """
    Generates a SQL query to fetch qualification and final scores for selected athletes
    from major competitions (from year 2023 onwards).

    :return: SQL query string
    """
    athlete_list_sql = ',\n            '.join(f"'{athlete}'" for athlete in TOP_ATHLETES)

    query = f"""
        SELECT
            r.competition_id AS competition_id,
            r.competition_name AS competition_name,
            r.event_name AS event_name,
            r.event_type AS event_type,
            r.year AS comp_year,
            r.competition_date AS comp_date,
            r.rank AS comp_rank,
            r.athlete_name AS athlete_name,
            r.total AS score,
            c.nation_name AS host_nation,
            c.nation_code AS host_nation_code,
            c.city AS host_city,
            c.competition_type AS comp_type
        FROM sai_shooting_final.shooting_results r
        INNER JOIN sai_shooting_final.shooting_athlete_events bio
            ON r.athlete_name COLLATE utf8mb4_unicode_520_ci = bio.athlete_name COLLATE utf8mb4_unicode_520_ci
            AND r.event_name COLLATE utf8mb4_unicode_520_ci = bio.athlete_event COLLATE utf8mb4_unicode_520_ci
        INNER JOIN sai_shooting_final.shooting_competition c
            ON r.competition_id = c.competition_id
        WHERE r.athlete_name IN (
            {athlete_list_sql}
        )
        AND r.year >= 2023
        AND r.event_type IN ('Qualification', 'Final')
        ORDER BY r.athlete_name, r.event_name;
    """
    return query


def read_all_shooting_results() -> str:
    """
    Generates a SQL query to fetch qualification scores from major competitions
    (from year 2023 onwards), across all athletes.

    :return: SQL query string
    """
    query = """
        SELECT
            r.competition_id AS competition_id,
            r.competition_name AS competition_name,
            r.event_name AS event_name,
            r.event_type AS event_type,
            r.year AS comp_year,
            r.competition_date AS comp_date,
            r.athlete_name AS athlete_name,
            r.total AS score,
            c.competition_type AS comp_type
        FROM sai_shooting_final.shooting_results r
        INNER JOIN sai_shooting_final.shooting_competition c
            ON r.competition_id = c.competition_id
        WHERE 
            r.year >= 2023
            AND r.event_type = 'Qualification'
            AND c.competition_type IN (
                'World Cup',
                'Asian Championships',
                'World Championships',
                'Asian Games',
                'World Cup Final',
                'Olympic Games'
            )
        ORDER BY r.competition_id;
    """
    return query
