"""
analysis.py

Service functions to extract and process shooting analysis
Date: 2025-08-04
"""

__author__ = "navin@gitaa.in"

import pandas as pd
from model.shooting.database import read_shooting_results,read_all_shooting_results,sai_db_engine

def load_shooting_results_data(engine) -> pd.DataFrame:
    """Load shooting results for both Qualification and Final."""
    query = read_shooting_results()
    return pd.read_sql(query, engine)


def load_all_results_data(engine) -> pd.DataFrame:
    """Load all results for top competitions."""
    query = read_all_shooting_results()
    return pd.read_sql(query, engine)


def clean_score_column(df: pd.DataFrame, score_column: str = 'score') -> pd.DataFrame:
    """Extract numeric part from score column and convert to float."""
    df[score_column] = df[score_column].astype(str).str.extract(r'^(\d+)').astype(float)
    return df


def prepare_ranked_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge qualification and final results to get last attained rank
    and identify whether it was from qualification or final round.
    """
    qual_df = df[df['event_type'] == 'Qualification'].copy()
    final_df = df[df['event_type'] == 'Final'].copy()

    qual_df = clean_score_column(qual_df)

    final_ranks = (
        final_df[[
            'competition_id', 'competition_name', 'event_name', 'athlete_name',
            'comp_rank', 'comp_year', 'comp_date', 'host_nation', 'host_nation_code',
            'host_city', 'comp_type'
        ]]
        .rename(columns={'comp_rank': 'last_attained_rank'})
    )

    qual_ranks = (
        qual_df[[
            'competition_id', 'competition_name', 'event_name', 'athlete_name',
            'comp_rank', 'score', 'comp_year', 'comp_date', 'host_nation', 'host_nation_code',
            'host_city', 'comp_type'
        ]]
        .drop_duplicates(subset=['competition_id', 'competition_name', 'event_name', 'athlete_name'])
        .rename(columns={'comp_rank': 'qual_rank'})
    )

    merged = pd.merge(
        qual_ranks, final_ranks,
        on=['competition_id', 'competition_name', 'event_name', 'athlete_name'],
        how='left',
        suffixes=('', '_final')
    )

    merged['last_attained_rank'] = merged['last_attained_rank'].fillna(merged['qual_rank'])

    merged['rank_type'] = merged.apply(
        lambda row: 'Final'
        if not pd.isna(row['last_attained_rank']) and not pd.isna(row['qual_rank']) and row['last_attained_rank'] != row['qual_rank']
        else 'Qualification',
        axis=1
    )

    merged.drop(columns=['qual_rank'], inplace=True)

    merged["comp_short_name"] = (
            merged['comp_type'] + '-' +
            merged['host_city'] + '-' +
            merged['comp_year'].astype(str)
    )

    final_columns = [
        'competition_id', 'competition_name', 'event_name', 'comp_year', 'comp_date',
        'athlete_name', 'score', 'last_attained_rank', 'rank_type',
        'host_nation', 'host_nation_code', 'host_city', 'comp_type','comp_short_name'
    ]

    return merged[final_columns]


def calculate_qmin_qmax(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each event, calculate qmin and qmax using mean ± std of top 8 qualification scores.
    """
    df = clean_score_column(df)

    top_8_scores = (
        df.sort_values(by=['competition_id', 'event_name', 'score'], ascending=[True, True, False])
          .groupby(['competition_id', 'event_name'])
          .head(8)
    )

    event_q_min_q_max = (
        top_8_scores.groupby('event_name')['score']
        .agg(['mean', 'std'])
        .rename(columns={'mean': 'mean_score', 'std': 'std_dev'})
        .assign(
            q_min=lambda x: x['mean_score'] - x['std_dev'],
            q_max=lambda x: x['mean_score'] + x['std_dev']
        )
        .reset_index()
        [['event_name', 'q_min', 'q_max']]
    )

    return event_q_min_q_max


def attach_q_min_q_max(merged_df: pd.DataFrame, q_minmax_df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach qmin and qmax to the merged DataFrame based on event_name.
    """
    return pd.merge(merged_df, q_minmax_df, on='event_name', how='left')


def populate_athlete_rank_distribution() -> None:
    """
    Calculates last attained ranks from raw qualification/final data,
    then computes and stores athlete-wise rank distribution.
    """

    # Step 1: Load data from database query
    query = read_shooting_results()
    df = pd.read_sql(query,con=sai_db_engine)

    # Step 2: Split by event type
    qual_df = df[df['event_type'] == 'Qualification'].copy()
    final_df = df[df['event_type'] == 'Final'].copy()

    # Step 3: Clean qualification scores
    qual_df = clean_score_column(qual_df)

    # Step 4: Prepare final rank data
    final_ranks = (
        final_df[[
            'competition_id', 'competition_name', 'event_name', 'athlete_name',
            'comp_rank', 'comp_year', 'comp_date', 'host_nation', 'host_nation_code',
            'host_city', 'comp_type'
        ]]
        .rename(columns={'comp_rank': 'last_attained_rank'})
    )

    # Step 5: Prepare qualification rank data
    qual_ranks = (
        qual_df[[
            'competition_id', 'competition_name', 'event_name', 'athlete_name',
            'comp_rank', 'score', 'comp_year', 'comp_date', 'host_nation', 'host_nation_code',
            'host_city', 'comp_type'
        ]]
        .drop_duplicates(subset=['competition_id', 'competition_name', 'event_name', 'athlete_name'])
        .rename(columns={'comp_rank': 'qual_rank'})
    )

    # Step 6: Merge qualification and final rank data
    merged = pd.merge(
        qual_ranks, final_ranks,
        on=['competition_id', 'competition_name', 'event_name', 'athlete_name'],
        how='left',
        suffixes=('', '_final')
    )

    merged['last_attained_rank'] = merged['last_attained_rank'].fillna(merged['qual_rank'])

    merged['rank_type'] = merged.apply(
        lambda row: 'Final'
        if not pd.isna(row['last_attained_rank']) and not pd.isna(row['qual_rank']) and row['last_attained_rank'] != row['qual_rank']
        else 'Qualification',
        axis=1
    )

    # Step 7: Drop temporary column
    merged.drop(columns=['qual_rank'], inplace=True)

    # Step 8: Categorize rank for pie chart
    def categorize(rank):
        if rank is None:
            return 'Not in Top 8'
        try:
            rank = int(rank)
        except:
            return 'Not in Top 8'
        if 1 <= rank <= 3:
            return 'Top 3'
        elif 4 <= rank <= 8:
            return 'Top 8'
        else:
            return 'Not in Top 8'

    merged['rank_category'] = merged['last_attained_rank'].apply(categorize)

    # Step 9: Group and aggregate (wide format)
    wide_df = (
        merged.groupby(['athlete_name', 'rank_category'])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={
            'Top 3': 'top_3',
            'Top 8': 'top_8',
            'Not in Top 8': 'not_in_top_8'
        })
    )

    # Step 10: Convert to long format (category, athlete_name, value)
    dist_df = pd.melt(
        wide_df,
        id_vars=['athlete_name'],
        value_vars=['top_3', 'top_8', 'not_in_top_8'],
        var_name='category',
        value_name='value'
    )

    return dist_df



