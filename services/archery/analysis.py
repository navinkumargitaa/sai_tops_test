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


def get_end_of_month_ranking():
    """
    Extract and process archery rankings to get end-of-October snapshot with status tagging.

    Returns:
        pd.DataFrame: A cleaned DataFrame with the latest October ranking per athlete per year.
    """
    # Step 1: Fetch ranking data from the database
    query = read_archery_ranking()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Step 2: Load induction details
    df_induction = pd.read_csv("/home/navin/Desktop/SAI/sai_tops_testing/data/archery/master_athlete_bio.csv")
    selected_cols = ['athlete_id', 'name', 'first_induction', 'first_exclusion', 'second_inclusion']
    df_induction_selected = df_induction[selected_cols]

    # Step 3: Parse all dates
    df['rank_date_issued'] = pd.to_datetime(df['rank_date_issued'], errors='coerce')
    df['year'] = df['rank_date_issued'].dt.year
    df['month'] = df['rank_date_issued'].dt.month

    date_columns = ['first_induction', 'first_exclusion', 'second_inclusion']
    for col in date_columns:
        df_induction_selected[col] = pd.to_datetime(
            df_induction_selected[col], format="%d/%m/%Y", errors='coerce'
        )

    # Step 4: Merge full ranking data with induction info before filtering
    merged_df = df.merge(df_induction_selected, on='athlete_id', how='left')

    # Step 5: Apply status tagging to all rows
    def get_status(row):
        rank_date = row['rank_date_issued']
        fi = row['first_induction']
        fe = row['first_exclusion']
        si = row['second_inclusion']

        if pd.isnull(fi):
            return 'no_induction'
        if rank_date < fi:
            return 'pre_induction'
        if pd.notnull(fe) and fi <= rank_date < fe:
            return 'post_induction'
        if pd.notnull(si) and fe <= rank_date < si:
            return 'excluded'
        if pd.notnull(si) and rank_date >= si:
            return 'post_induction'
        if pd.notnull(fe) and rank_date >= fe:
            return 'excluded'
        return 'post_induction'

    merged_df['ranking_status'] = merged_df.apply(get_status, axis=1)

    # TODO: Change the logic if needed
    # Step 6: Filter for October rankings only
    # october_df = merged_df[merged_df['month'] == 9]
    #
    # # Step 7: Get the last October ranking per athlete per year
    # end_of_september = (
    #     october_df
    #     .sort_values('rank_date_issued')
    #     .groupby(['athlete_id', 'year'])
    #     .tail(1)
    # )

    # Step 6: Get the last ranking of each month per athlete per year
    monthly_last_ranking = (
        merged_df
        .sort_values('rank_date_issued')
        .groupby(['athlete_id', 'year', 'month'], as_index=False)
        .tail(1)
    )

    # Step 8: Select required columns
    final_df = monthly_last_ranking[[
        'athlete_id',
        'year',
        'athlete_name',
        'current_rank',
        'rank_date_issued',
        'ranking_status'
    ]]

    return final_df


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

    # Step 3: Fill empty comp_short_name
    df['comp_short_name'] = df['comp_short_name'].fillna('').str.strip()

    # Apply transformation only to rows with missing or empty comp_short_name
    mask = df['comp_short_name'] == ''
    df.loc[mask, 'comp_short_name'] = (
        df.loc[mask, 'comp_full_name'].str.split().str[:3].str.join(' ') + " - " +
        df.loc[mask, 'comp_place'].fillna('Unknown') + " " +
        df.loc[mask, 'comp_date'].dt.year.astype(str)
    )

    return df

def fill_missing_short_name(
    df: pd.DataFrame,
    short_name_col: str,
    full_name_col: str,
    place_col: str,
    date_col: str,
    num_words_from_full_name: int = 3,
    default_place: str = "Unknown"
) -> pd.DataFrame:
    """
    Fills missing or empty values in the short name column using a pattern based on
    the full name, place, and year from a date column.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        short_name_col (str): Column name for the short name to be filled.
        full_name_col (str): Column name for the full name to extract from.
        place_col (str): Column name for the place.
        date_col (str): Column name for the date.
        num_words_from_full_name (int): Number of words to take from the full name (default: 3).
        default_place (str): Value to use when place is missing (default: "Unknown").

    Returns:
        pd.DataFrame: DataFrame with updated short name column.
    """
    # Ensure date is in datetime format
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Clean and fill missing short names
    df[short_name_col] = df[short_name_col].fillna('').str.strip()

    # Identify missing short name entries
    mask = df[short_name_col] == ''

    # Build short name for missing entries
    df.loc[mask, short_name_col] = (
        df.loc[mask, full_name_col].fillna('')
        .str.split().str[:num_words_from_full_name].str.join(' ') + " - " +
        df.loc[mask, place_col].fillna(default_place) + " " +
        df.loc[mask, date_col].dt.year.fillna(0).astype(int).astype(str)
    )

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



# ---- Qualification Data Processing ----
def prepare_qualification_data():
    """
    Load, clean, and process qualification data.
    Returns:
        pd.DataFrame: Qualification data with computed average arrow score.
    """
    # Load data
    qual_query = read_archery_qual_results()
    qual_df = pd.read_sql_query(qual_query, con=sai_db_engine)

    # Clean comp_name
    qual_df = fill_missing_short_name(
        qual_df,
        short_name_col='comp_name',
        full_name_col='comp_full_name',
        place_col='comp_place',
        date_col='comp_date'
    )

    # Define row-wise logic for average arrow score
    def compute_avg_arrow_score(row):
        if row['gold_header'] == '10+X':
            return round(row['qual_score'] / 72, 2)
        elif row['gold_header'] == '11':
            return round((row['qual_score'] - row['qual_gold']) / 60, 2)
        else:
            return None  # or np.nan for unexpected values

    # Apply the function to each row
    qual_df['avg_arrow_score_qualification'] = qual_df.apply(compute_avg_arrow_score, axis=1)

    return qual_df




# ---- Elimination Data Processing ----
def prepare_elimination_data():
    """
    Load, clean, and process elimination data.
    Returns:
        pd.DataFrame: Elimination data with computed average arrow score.
    """
    # Load data
    elem_query = read_archery_elem_results()
    elem_df = pd.read_sql_query(elem_query, con=sai_db_engine)

    # Clean comp_name
    elem_df = fill_missing_short_name(
        elem_df,
        short_name_col='comp_name',
        full_name_col='comp_full_name',
        place_col='comp_place',
        date_col='comp_date'
    )

    # Compute arrow average from set_points
    elem_df['avg_arrow_score_elemination_round'] = pd.to_numeric(
        elem_df['set_points'].apply(compute_avg_arrow_score), errors='coerce'
    ).round(2)

    return elem_df

# ---- Final Orchestrator ----
def get_arrow_average():
    """
    Get the overall arrow average per athlete per competition by merging
    qualification and elimination scores.

    Returns:
        pd.DataFrame: Final DataFrame with qual, elim, overall averages, comp_date, and year.
    """
    # Prepare both datasets
    qual_df = prepare_qualification_data()
    elem_df = prepare_elimination_data()

    # Ensure comp_date is datetime
    qual_df['comp_date'] = pd.to_datetime(qual_df['comp_date'], errors='coerce')
    elem_df['comp_date'] = pd.to_datetime(elem_df['comp_date'], errors='coerce')

    # Add year column to qualification
    qual_df['comp_year'] = qual_df['comp_date'].dt.year

    # Aggregate elimination scores and round to 2 decimals
    elem_avg_df = (
        elem_df.groupby(['athlete_id', 'athlete_name', 'competition_id', 'comp_name'])[
            'avg_arrow_score_elemination_round'
        ]
        .mean()
        .round(2)  # Round the aggregated mean
        .reset_index()
        .rename(columns={'avg_arrow_score_elemination_round': 'elem_avg_arrow'})
    )

    # Select qualification scores + comp_date and comp_year, and round to 2 decimals
    qual_avg_df = qual_df[[
        'athlete_id', 'athlete_name', 'competition_id', 'comp_name',
        'avg_arrow_score_qualification', 'comp_date', 'comp_year'
    ]].copy()

    qual_avg_df['avg_arrow_score_qualification'] = qual_avg_df['avg_arrow_score_qualification'].round(2)

    # Rename after rounding
    qual_avg_df = qual_avg_df.rename(columns={'avg_arrow_score_qualification': 'qual_avg_arrow'})

    # Merge both
    merged = pd.merge(
        qual_avg_df,
        elem_avg_df,
        on=['athlete_id', 'athlete_name', 'competition_id', 'comp_name'],
        how='outer'
    )

    # Compute overall average
    # Compute overall average
    merged['competition_avg_arrow'] = merged[['qual_avg_arrow', 'elem_avg_arrow']].mean(axis=1).round(2)

    # Ensure proper datetime format before returning
    merged['comp_date'] = pd.to_datetime(merged['comp_date'], errors='coerce')
    merged['comp_year'] = pd.to_numeric(merged['comp_year'], errors='coerce').astype('Int64')

    # Final DataFrame
    final_df = merged[[
        'athlete_id', 'athlete_name', 'competition_id', 'comp_name',
        'comp_date', 'comp_year',
        'qual_avg_arrow', 'elem_avg_arrow', 'competition_avg_arrow'
    ]]

    # Melt the DataFrame to convert the average columns into rows
    df_long = pd.melt(
        final_df,
        id_vars=[
            'athlete_id', 'athlete_name', 'competition_id',
            'comp_name', 'comp_date', 'comp_year'
        ],
        value_vars=[
            'qual_avg_arrow', 'elem_avg_arrow', 'competition_avg_arrow'
        ],
        var_name='type_arrow_avg',
        value_name='arrow_average'
    )

    # Optional: Clean up the 'type_arrow_avg' values for readability
    df_long['type_arrow_avg'] = df_long['type_arrow_avg'].replace({
        'qual_avg_arrow': 'qualification',
        'elem_avg_arrow': 'elimination',
        'competition_avg_arrow': 'competition'
    })
    df_long = df_long.dropna()
    return df_long

