"""
This module creates a SQLAlchemy engine for the SAI_Badminton Database
and provides a query function for badminton athletes to perform various tasks.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

# SQLAlchemy engine for new DB
sai_db_engine = create_engine("mysql+pymysql://root:root@localhost:3306/sai_badminton_viz_final")
SessionLocal = sessionmaker(bind=sai_db_engine)

def read_singles_tournament_finishes():
    """
    Fetch tournament finish data for selected singles athletes.
    Returns a DataFrame.
    """
    athlete_ids = [83950, 68870, 73173, 69093, 59687, 74481, 58664,
                   68322, 99042, 91807, 97168, 70595, 82572]

    query = """
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
          ON a.tournament_id = b.tournament_id AND a.athlete_id = b.athlete_id
        WHERE a.athlete_id IN ({})
    """.format(", ".join(map(str, athlete_ids)))

    return pd.read_sql(query, sai_db_engine)

