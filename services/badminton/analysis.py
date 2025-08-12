"""
analysis.py

Service functions to extract and process badminton data

"""

__author__ = "navin@gitaa.in"

import pandas as pd
import numpy as np
from fuzzywuzzy import process
import calendar
from datetime import date
import itertools
from model.badminton.database import SessionLocal
from model.badminton.database import read_singles_ranking_progression,read_doubles_ranking_progression,sai_db_engine
from model.badminton.database import read_all_tournament_details,read_singles_notable_wins,read_singles_ranking_table
from model.badminton.database import read_singles_tournament_finishes,read_doubles_tournament_finishes



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


def parse_date_range_df(df, date_col='date', year_col='year'):
    """Parses date range strings into start_date and end_date columns."""
    month_map = {month.lower(): i for i, month in enumerate(calendar.month_name) if month}

    def parse_range(range_str, year):
        range_str = str(range_str).strip().lower()
        try:
            start_str, end_str = [part.strip() for part in range_str.split("-")]

            # Extract start day/month
            if " " in start_str:
                start_day, start_month = start_str.split()
            else:
                start_day = start_str
                start_month = None

            # Extract end day/month
            end_day, end_month = end_str.split()

            # If start month missing, use end month
            if start_month is None:
                start_month = end_month

            # Convert to numbers
            start_month_num = month_map[start_month]
            end_month_num = month_map[end_month]

            start_date = date(int(year), start_month_num, int(start_day))
            end_date = date(int(year), end_month_num, int(end_day))

            # Handle rollover errors
            if end_date < start_date:
                raise ValueError("End date before start date.")

            return start_date, end_date
        except Exception:
            return None, None  # Gracefully handle bad/missing data

    # Apply to DataFrame
    df[['start_date', 'end_date']] = df.apply(
        lambda row: pd.Series(parse_range(row[date_col], row[year_col])),
        axis=1
    )

    return df


def process_tournament_grade():
    """
    For tournaments with missing grades:
    - Parse date ranges into start_date and end_date
    - Fill missing grades via keyword rules, exact matches, and fuzzy matching
    """

    # Load mapping
    grade_mapping_data = pd.read_csv(
        '/home/navin/Desktop/SAI/sai_tops_testing/data/badminton/grade_mapping.csv'
    )

    # Fetch tournament details
    query = read_all_tournament_details()
    tournament_data = pd.read_sql_query(query, con=sai_db_engine)

    # Parse date ranges before anything else
    tournament_data = parse_date_range_df(tournament_data, 'date', 'year')

    # Clean mapping names
    grade_mapping_data['tournament_name_clean'] = (
        grade_mapping_data['tournament_name'].str.lower().str.strip()
    )

    # Only rows with missing grade
    missing_data = tournament_data[tournament_data['grade'].isna()].copy()
    missing_data['name_clean'] = missing_data['name'].str.lower().str.strip()

    # Prepare for fuzzy matching
    mapping_names_list = grade_mapping_data['tournament_name_clean'].tolist()
    mapping_grades_list = grade_mapping_data['grade'].tolist()

    def get_grade(tourn_name, mapping_names, mapping_grades, threshold=85):
        tourn_name = tourn_name.lower()

        # Keyword rules
        if 'junior' in tourn_name or 'u19' in tourn_name or 'u17' in tourn_name:
            return 'Junior'
        if 'commonwealth' in tourn_name:
            return 'Grade 1'

        # Exact matches
        exact_matches = {
            'olympic games': 'Grade 1',
            'bwf world championships': 'Grade 1',
            'bwf sudirman cup finals': 'Grade 1',
            'bwf thomas & uber cup': 'Grade 1',
            'badminton asia championships': '1000',
            'badminton asia mixed team championship': '1000',
            'bwf world tour finals': 'Grade 2: Level 1',
            'asian games': '1000',
        }
        for key, val in exact_matches.items():
            if key in tourn_name:
                return val

        # Fuzzy match
        match, score = process.extractOne(tourn_name, mapping_names)
        if score >= threshold:
            return mapping_grades[mapping_names.index(match)]
        return None

    # Apply only to missing grades
    missing_data['matched_grade'] = missing_data['name_clean'].apply(
        lambda x: get_grade(x, mapping_names_list, mapping_grades_list)
    )

    # Ensure target column exists
    if 'new_grade' not in tournament_data.columns:
        tournament_data['new_grade'] = None

    # Copy existing grade values
    tournament_data.loc[tournament_data['grade'].notna(), 'new_grade'] = tournament_data['grade']

    # Fill missing ones from matched_grade
    tournament_data.loc[missing_data.index, 'new_grade'] = missing_data['matched_grade']

    return tournament_data


def add_win_flag_and_names(df):
    """
    Adds win_flag, athlete_name, opponent_name, and opponent_id columns
    based on athlete_id, team IDs, and winner info.
    """
    df = df.copy()

    def process_row(row):
        if row['athlete_id'] == row['team_1_player_1_id']:
            return pd.Series([
                1 if row.get('winner') == 1 else 0,
                row['team_1_player_1_name'],
                row['team_2_player_1_name'],
                row['team_2_player_1_id']
            ])
        elif row['athlete_id'] == row['team_2_player_1_id']:
            return pd.Series([
                1 if row.get('winner') == 2 else 0,
                row['team_2_player_1_name'],
                row['team_1_player_1_name'],
                row['team_1_player_1_id']
            ])
        else:
            return pd.Series([None, None, None, None])

    df[['win_flag', 'athlete_name', 'opponent_name', 'opponent_id']] = df.apply(process_row, axis=1)

    return df


def prepare_final_match_df(df):
    """
    Cleans and reorders match dataframe:
    - Drops unnecessary columns
    - Ensures start_date is datetime
    - Adds week_number and year
    - Reorders columns to final order
    """
    df = df.copy()

    # Ensure start_date is datetime
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')

    # Add week number & year
    df['week_number'] = df['start_date'].dt.isocalendar().week
    df['year'] = df['start_date'].dt.year

    # Drop unwanted columns
    drop_cols = [
        'tournament_match_id', 'draw_name_full', 'winner',
        'team_1_player_1_id', 'team_1_player_1_name',
        'team_2_player_1_id', 'team_2_player_1_name',
        'date', 'end_date'
    ]
    df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True, errors='ignore')

    # Final column order
    final_order = [
        'tournament_id', 'tournament_name', 'tournament_grade',
        'round_name', 'athlete_id', 'athlete_name',
        'opponent_id', 'opponent_name', 'win_flag',
        'start_date', 'week_number', 'year'
    ]
    df = df[[col for col in final_order if col in df.columns]]

    return df


def add_week_year_columns(df):
    """
    Convert 'date' column to datetime and add ISO week number and year columns.

    Parameters:
        df (pd.DataFrame): DataFrame with at least a 'date' column.

    Returns:
        pd.DataFrame: Updated DataFrame with 'week_number' and 'year' columns.
    """
    # Ensure date column is datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Add ISO week number
    df['week_number'] = df['date'].dt.isocalendar().week

    # Add year
    df['year'] = df['date'].dt.year

    return df





def attach_ranks_without_merge_asof(df, data_rank):
    """
    Attach 'world_ranking' to matches for both athlete and opponent
    as of the match start_date, without using pandas.merge_asof.

    Parameters:
        df (pd.DataFrame): Match data with 'athlete_id', 'opponent_id', 'start_date'.
        data_rank (pd.DataFrame): Ranking data with 'athlete_id', 'date', 'world_ranking'.

    Returns:
        pd.DataFrame: Original df with two new columns:
                      'athlete_world_ranking' and 'opponent_world_ranking'.
    """

    df = df.copy()
    ranks = data_rank.copy()

    # --- Normalize types ---
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    ranks['date'] = pd.to_datetime(ranks['date'], errors='coerce')

    df['athlete_id'] = df['athlete_id'].astype(str)
    df['opponent_id'] = df['opponent_id'].astype(str)
    ranks['athlete_id'] = ranks['athlete_id'].astype(str)

    # --- Prepare ranks: one row per (athlete_id, date), sorted by date ---
    ranks = (
        ranks[['athlete_id', 'date', 'world_ranking']]
        .dropna(subset=['athlete_id', 'date'])
        .drop_duplicates(subset=['athlete_id', 'date'], keep='last')
        .sort_values(['athlete_id', 'date'], kind='mergesort')
        .reset_index(drop=True)
    )

    # Pre-split ranks into arrays per athlete for fast lookup
    ranks_by_id = {
        aid: (g['date'].to_numpy(), g['world_ranking'].to_numpy())
        for aid, g in ranks.groupby('athlete_id', sort=False)
    }

    def asof_lookup(ids: pd.Series, dates: pd.Series, right_map):
        out = np.full(len(ids), np.nan, dtype='float64')
        for k, idx in ids.groupby(ids, sort=False).groups.items():
            rd = right_map.get(k)
            if rd is None:
                continue
            r_dates, r_vals = rd
            pos = np.searchsorted(r_dates, dates.iloc[idx].to_numpy(), side='right') - 1
            valid = pos >= 0
            if np.any(valid):
                out_idx = np.asarray(list(idx))[valid]
                out[out_idx] = r_vals[pos[valid]]
        return pd.Series(out, index=ids.index)

    # Attach rankings for athlete and opponent
    df['athlete_world_ranking'] = asof_lookup(df['athlete_id'], df['start_date'], ranks_by_id)
    df['opponent_world_ranking'] = asof_lookup(df['opponent_id'], df['start_date'], ranks_by_id)

    return df

def build_notable_wins_with_ranks():
    """
    Fetch singles notable wins and rankings from the database,
    process them, and attach athlete/opponent world rankings.
    """
    df = pd.read_sql_query(read_singles_notable_wins(), con=sai_db_engine)
    ranks = pd.read_sql_query(read_singles_ranking_table(), con=sai_db_engine)

    df = add_win_flag_and_names(df)
    df = prepare_final_match_df(df)
    ranks = add_week_year_columns(ranks)

    df = attach_ranks_without_merge_asof(df, ranks)

    # Ensure start_date is datetime
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')

    # Compute year
    df['year'] = df['start_date'].dt.year

    # Final column order
    final_cols = [
        'tournament_id', 'tournament_name', 'tournament_grade', 'round_name',
        'athlete_id', 'athlete_name', 'opponent_id', 'opponent_name',
        'win_flag', 'start_date','year'
        'athlete_world_ranking', 'opponent_world_ranking'   
    ]
    df_final = df[[col for col in final_cols if col in df.columns]]

    return df_final


"""
Below is the scripts for tournament finishes
"""

def read_singles_tournament_finishes_data(main_data, filter_data):
    """

    :return:
    """
    query = read_singles_tournament_finishes()
    df = pd.read_sql_query(query,con=sai_db_engine)
    doubles_filtered = pd.read_csv('/home/navin/Desktop/SAI/sai_tops_testing/data/badminton/singles_category_filtered.csv')
    return df.merge(doubles_filtered, on=["athlete_id", "category"], how="inner")

def read_doubles_tournament_finishes_data(main_data, filter_data):
    """

    :return:
    """
    query = read_doubles_tournament_finishes()
    df = pd.read_sql_query(query,con=sai_db_engine)
    doubles_filtered = pd.read_csv('/home/navin/Desktop/SAI/sai_tops_testing/data/badminton/doubles_category_filtered.csv')
    return df.merge(doubles_filtered, on=["athlete_id", "category"], how="inner")


def clean_and_transform_data(data):
    """Map positions, clean data, and create pivot table."""
    position_map = {
        "R32": "R32", "R16": "R16", "QF": "QF", "N/A": "Team", "1st": "F", "3/4": "SF",
        "3rd": "SF", "2nd": "F", "Qual. R16": "<R32", "R64": "<R32", "Qual. QF": "<R32",
        "Qual. R32": "<R32", "-": None, "R128": "<R32", "Round of 16": "R16", "R3": "<R32",
        "Qual. R128": "<R32", "Qual. R64": "<R32", "R4": "R32", "Final": "F", "R2": "<R32",
        "SF": "SF", "R5": "R16", "Quarterfinals": "QF", "Group A": "<R32"
    }

    # Replace values
    data['final_position'] = data['final_position'].replace(position_map)
    data['tournament_grade'] = data['tournament_grade'].replace('final', '1000')
    data = data.dropna(subset=['final_position', 'tournament_grade'])

    # Create pivot table
    pivot_df = data.pivot_table(
        index=['athlete_id', 'athlete_name', 'tournament_year', 'tournament_grade'],
        columns='final_position',
        aggfunc='size',
        fill_value=0
    )

    # Unstack grade level into columns
    pivot_df = pivot_df.unstack(level='tournament_grade', fill_value=0)

    # Flatten MultiIndex columns
    pivot_df.columns = [f"{grade}_{pos}" for pos, grade in pivot_df.columns]

    return pivot_df.reset_index()

def make_static_columns(df, order_grade_levels, order_position_stages):
    """Ensure all grade-position combinations exist and reorder columns."""
    desired_cols = [f"{grade}_{pos}" for grade, pos in itertools.product(order_grade_levels, order_position_stages)]
    for col in desired_cols:
        if col not in df.columns:
            df[col] = 0
    return df[['athlete_id', 'athlete_name', 'tournament_year'] + desired_cols]

def process_singles_tournament_finishes():
    """Main processing pipeline."""

    query = read_singles_tournament_finishes()
    df = pd.read_sql_query(query, con=sai_db_engine)
    doubles_filtered = pd.read_csv(
        '/home/navin/Desktop/SAI/sai_tops_testing/data/badminton/singles_category_filtered.csv')
    data= df.merge(doubles_filtered, on=["athlete_id", "category"], how="inner")

    order_grade_levels = ["Junior", "G3", "100", "300", "500", "750", "1000", "Grade 1"]
    order_position_stages = ["<R32", "R32", "R16", "QF", "SF", "F"]

    pivot_df = clean_and_transform_data(data)
    return make_static_columns(pivot_df, order_grade_levels, order_position_stages)

def process_doubles_tournament_finishes():
    """Main processing pipeline."""
    query = read_doubles_tournament_finishes()
    df = pd.read_sql_query(query, con=sai_db_engine)
    doubles_filtered = pd.read_csv(
        '/home/navin/Desktop/SAI/sai_tops_testing/data/badminton/doubles_category_filtered.csv')
    data =  df.merge(doubles_filtered, on=["athlete_id", "category"], how="inner")


    order_grade_levels = ["Junior", "G3", "100", "300", "500", "750", "1000", "Grade 1"]
    order_position_stages = ["<R32", "R32", "R16", "QF", "SF", "F"]

    pivot_df = clean_and_transform_data(data)
    return make_static_columns(pivot_df, order_grade_levels, order_position_stages)












