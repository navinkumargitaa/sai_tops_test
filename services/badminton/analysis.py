"""
analysis.py

Service functions to extract and process badminton data

"""

__author__ = "navin@gitaa.in"

import pandas as pd
from model.badminton.database import SessionLocal
from model.badminton.database import read_singles_ranking_progression,read_doubles_ranking_progression,sai_db_engine



def get_singles_ranking_data():
    """

    :return: Processed data for viz table
    """

    query = read_singles_ranking_progression()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Convert to datetime
    df['ranking_date'] = pd.to_datetime(df['ranking_date'], errors='coerce')
    df['year'] = df['ranking_date'].dt.year

    # Extract year into a new column
    df['year'] = df['ranking_date'].dt.year

    # Step 1: Sort by athlete and date
    df = df.sort_values(['athlete_id', 'ranking_date'])

    df['world_tour_ranking'] = df.groupby('athlete_id')['world_tour_ranking'].ffill()

    return df

def get_doubles_ranking_data():
    """

    :return: Processed data for viz table
    """

    query = read_doubles_ranking_progression()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Convert to datetime
    df['ranking_date'] = pd.to_datetime(df['ranking_date'], errors='coerce')
    df['year'] = df['ranking_date'].dt.year

    # Extract year into a new column
    df['year'] = df['ranking_date'].dt.year

    # Step 1: Sort by athlete and date
    df = df.sort_values(['composite_team_id', 'ranking_date'])

    df['world_tour_ranking'] = df.groupby('composite_team_id')['world_tour_ranking'].ffill()

    return df


