"""
analysis.py

Service functions to extract and process archery ranking and arrow score data
for end-of-October snapshots, competition rankings, and average arrow performance.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

import pandas as pd
from model.archery.database import (
    sai_db_engine,
    read_archery_ranking,
    read_archery_comp_ranking,
    read_archery_qual_results,
    read_archery_elem_results
)


def get_end_of_october_ranking():
    """
    Extract and process archery rankings to get end-of-October snapshot.

    Returns:
        pd.DataFrame: A cleaned DataFrame with the latest October ranking per athlete per year.
    """
    # Step 1: Fetch ranking data from the database
    query = read_archery_ranking()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Step 2: Parse rank date and extract year/month
    df['rank_date_issued'] = pd.to_datetime(df['rank_date_issued'], errors='coerce')
    df['year'] = df['rank_date_issued'].dt.year
    df['month'] = df['rank_date_issued'].dt.month

    # Step 3: Filter for October rankings only
    october_df = df[df['month'] == 10]

    # Step 4: Get the last ranking record for each athlete per year
    end_of_october = (
        october_df
        .sort_values('rank_date_issued')
        .groupby(['athlete_id', 'year'])
        .tail(1)
    )

    # Step 5: Keep only relevant columns
    end_of_october = end_of_october[['athlete_id', 'year', 'rank', 'points', 'rank_date_issued']]
    return end_of_october


def get_competition_ranking():
    """
    Extract and transform athlete competition rankings.

    Returns:
        pd.DataFrame: Transformed DataFrame with added competition short name for visualizations.
    """
    # Step 1: Fetch competition ranking data
    query = read_archery_comp_ranking()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Step 2: Convert date column to datetime
    df['comp_date'] = pd.to_datetime(df['comp_date'])

    # Step 3: Generate visualization-friendly short name
    df['comp_new_short_name'] = df['comp_place'] + ' ' + df['comp_date'].dt.year.astype(str)

    return df


def compute_avg_arrow_score(sp_str):
    """
    Compute average arrow score from set points string.

    Args:
        sp_str (str): A string like '27|29|28|30' where each value is points in a set.

    Returns:
        float or None: Average arrow score or None if parsing fails.
    """
    if pd.isna(sp_str):
        return None
    try:
        sets = sp_str.split('|')
        total_points = sum(int(s) for s in sets)
        total_arrows = len(sets) * 3  # 3 arrows per set
        return round(total_points / total_arrows, 2)
    except Exception:
        return None


def get_arrow_average():
    """
    Extract and compute qualification, elimination, and overall average arrow scores.

    Returns:
        pd.DataFrame: Merged DataFrame containing all average scores per athlete per competition.
    """
    # Step 1: Load qualification and elimination data
    query_qual = read_archery_qual_results()
    query_elem = read_archery_elem_results()

    qual_data = pd.read_sql_query(query_qual, con=sai_db_engine)
    elem_data = pd.read_sql_query(query_elem, con=sai_db_engine)

    # Step 2: Prepare and format date & competition name fields
    for df in [qual_data, elem_data]:
        df['comp_date'] = pd.to_datetime(df['comp_date'])
        df['comp_new_short_name'] = df['comp_place'] + ' ' + df['comp_date'].dt.year.astype(str)

    # Step 3: Calculate qualification average score (fixed 72 arrows)
    qual_data['avg_arrow_score_qualification'] = round(qual_data['qual_score'] / 72, 2)

    # Step 4: Calculate elimination round average score using set points
    elem_data['avg_arrow_score_elemination_round'] = elem_data['set_points'].apply(compute_avg_arrow_score)

    # Step 5: Convert to numeric and handle missing/invalid values
    elem_data['avg_arrow_score_elemination_round'] = pd.to_numeric(
        elem_data['avg_arrow_score_elemination_round'], errors='coerce'
    )
    qual_data['avg_arrow_score_qualification'] = pd.to_numeric(
        qual_data['avg_arrow_score_qualification'], errors='coerce'
    )

    # Step 6: Aggregate elimination scores per athlete per competition
    elim_avg = (
        elem_data.groupby(['athlete_id', 'athlete_name', 'competition_id', 'comp_new_short_name'])[
            'avg_arrow_score_elemination_round'
        ]
        .mean()
        .reset_index()
        .rename(columns={'avg_arrow_score_elemination_round': 'elem_avg_arrow'})
    )

    # Step 7: Select and rename qualification scores
    qual_avg = qual_data[[
        'athlete_id', 'athlete_name', 'competition_id', 'comp_new_short_name', 'avg_arrow_score_qualification'
    ]].rename(columns={'avg_arrow_score_qualification': 'qual_avg_arrow'})

    # Step 8: Merge qualification and elimination scores
    merged = pd.merge(
        elim_avg,
        qual_avg,
        on=['athlete_id', 'athlete_name', 'competition_id', 'comp_new_short_name'],
        how='outer'
    )

    # Step 9: Calculate overall average arrow score
    merged['competition_avg_arrow'] = merged[['qual_avg_arrow', 'elem_avg_arrow']].mean(axis=1)

    # Step 10: Final column selection and ordering
    final_df = merged[[
        'athlete_id', 'athlete_name', 'competition_id', 'comp_new_short_name',
        'qual_avg_arrow', 'elem_avg_arrow', 'competition_avg_arrow'
    ]]

    return final_df
