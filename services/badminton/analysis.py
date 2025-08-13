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
from datetime import timedelta
import itertools
from model.badminton.database import SessionLocal
from model.badminton.database import read_singles_ranking_progression,read_doubles_ranking_progression,sai_db_engine
from model.badminton.database import read_all_tournament_details,read_singles_notable_wins,read_singles_ranking_table
from model.badminton.database import read_singles_tournament_finishes,read_doubles_tournament_finishes
from model.badminton.database import read_doubles_notable_wins,read_doubles_ranking_table



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


"""
Below is the scripts for tournament finishes
"""


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



'''
Notable wins
'''


# ----------------------------
# Step 1: Add win_flag and names
# ----------------------------
def add_win_flag_and_names(df):
    def process_row(row):
        if row['athlete_id'] == row['team_1_player_1_id']:
            return pd.Series([
                'Won' if row.get('winner') == 1 else 'Lost',
                row['team_1_player_1_name'],
                row['team_2_player_1_name'],
                row['team_2_player_1_id']
            ])
        elif row['athlete_id'] == row['team_2_player_1_id']:
            return pd.Series([
                'Won' if row.get('winner') == 2 else 'Lost',
                row['team_2_player_1_name'],
                row['team_1_player_1_name'],
                row['team_1_player_1_id']
            ])
        else:
            return pd.Series([None, None, None, None])

    df[['win_flag', 'athlete_name', 'opponent_name', 'opponent_id']] = df.apply(process_row, axis=1)
    return df

# ----------------------------
# Step 2: Drop unnecessary columns and reorder
# ----------------------------
def clean_and_reorder_columns(df):
    drop_cols = [
        'tournament_match_id', 'draw_name_full', 'winner',
        'team_1_player_1_id', 'team_1_player_1_name',
        'team_2_player_1_id', 'team_2_player_1_name',
        'date', 'year'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')

    final_order = [
        'tournament_id', 'tournament_name', 'tournament_grade',
        'round_name', 'athlete_id', 'athlete_name',
        'opponent_id', 'opponent_name', 'win_flag',
        'start_date'
    ]
    return df[final_order]

# ----------------------------
# Step 3: Add week_number and year from a date column
# ----------------------------
def add_date_features(df, date_col='start_date'):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df['week_number'] = df[date_col].dt.isocalendar().week
    df['year'] = df[date_col].dt.year
    return df

# ----------------------------
# Step 5: Pipeline Execution
# ----------------------------

def add_latest_tuesday(df, date_col, output_col="latest_tuesday", strictly_before=False):
    """
    Add a column with the most recent Tuesday on/before each date.

    - df: DataFrame
    - date_col: name of the input date/datetime column
    - output_col: name of the output column to create
    - strictly_before: if True, return the Tuesday *before* the date even when it's Tuesday
    """
    s = df[date_col]

    # Ensure datetimelike; converts Python datetime/date or strings to datetime64[ns]
    if not pd.api.types.is_datetime64_any_dtype(s):
        s = pd.to_datetime(s, errors="coerce")

    if s.isna().any():
        bad_idx = s[s.isna()].index.tolist()
        raise ValueError(f"{date_col} has unparseable values at rows: {bad_idx}")

    # Monday=0, Tuesday=1, ..., Sunday=6
    days_back = (s.dt.weekday - 1) % 7
    if strictly_before:
        # If already Tuesday, go back 7 days to the previous Tuesday
        days_back = days_back.where(days_back != 0, 7)

    latest = s - pd.to_timedelta(days_back, unit="D")

    out = df.copy()
    out[output_col] = latest
    return out



def attach_rank_simple(
    df,
    data_rank,
    id_col="athlete_id",
    date_col="latest_tuesday",   # in df
    rank_date_col="date",        # in data_rank
    rank_col="world_ranking",    # in data_rank
    out_col="world_ranking",
    strictly_before=False        # False => on-or-before; True => strictly before
):
    # Minimal copies
    left = df[[id_col, date_col]].copy()
    right = data_rank[[id_col, rank_date_col, rank_col]].copy()

    # Parse datetimes
    left[date_col] = pd.to_datetime(left[date_col], errors="coerce")
    right[rank_date_col] = pd.to_datetime(right[rank_date_col], errors="coerce")

    # Unify ID dtype
    try:
        left[id_col] = left[id_col].astype("Int64")
        right[id_col] = right[id_col].astype("Int64")
    except Exception:
        left[id_col] = left[id_col].astype(str)
        right[id_col] = right[id_col].astype(str)

    # Prepare query rows
    A = left.rename(columns={date_col: "__date__"}).copy()
    A["_q"] = True
    A[rank_col] = pd.NA              # placeholder
    A["__rank_date__"] = pd.NaT

    # Prepare rank rows
    B = right.rename(columns={rank_date_col: "__date__"}).copy()
    B["_q"] = False
    B["__rank_date__"] = B["__date__"]

    # Same-day behavior via sort order
    if strictly_before:
        A["_ord"] = 0; B["_ord"] = 1   # skip same-day
    else:
        A["_ord"] = 1; B["_ord"] = 0   # include same-day

    C = pd.concat([A, B], ignore_index=True, sort=False)
    C = C.sort_values([id_col, "__date__", "_ord"])

    # Avoid name collision if out_col == rank_col
    out_value_col = out_col if out_col != rank_col else "__out_value__"

    # Forward-fill within athlete
    C[out_value_col] = C.groupby(id_col)[rank_col].ffill()
    C["__rank_date__"] = C.groupby(id_col)["__rank_date__"].ffill()

    # Keep only query rows and rename
    drop_cols = ["_q", "_ord"]
    # Only drop rank_col if it's different from out_value_col
    if rank_col != out_value_col:
        drop_cols.append(rank_col)

    out = (
        C[C["_q"]]
        .drop(columns=drop_cols)
        .rename(columns={
            "__date__": date_col,
            "__rank_date__": f"{rank_date_col}_matched",
            out_value_col: out_col
        })
    )

    # Merge back to original df
    return df.merge(
        out[[id_col, date_col, out_col, f"{rank_date_col}_matched"]],
        on=[id_col, date_col],
        how="left"
    )

def process_singles_notable_wins():
    data = pd.read_sql_query(read_singles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_singles_ranking_table(), con=sai_db_engine)

    df = add_win_flag_and_names(data)
    df = clean_and_reorder_columns(df)
    df = add_date_features(df, date_col='start_date')

    # Ensure data_rank dates are parsed
    data_rank = data_rank.copy()
    data_rank['date'] = pd.to_datetime(data_rank['date'], errors='coerce')
    df["athlete_id"] = pd.to_numeric(df["athlete_id"], errors="coerce").astype("Int64")
    df["opponent_id"] = pd.to_numeric(df["opponent_id"], errors="coerce").astype("Int64")
    # Example: ensure correct datetime parsing
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    # Format as YYYY-MM-DD
    df["start_date"] = df["start_date"].dt.strftime("%Y-%m-%d")

    # Ensure same integer type
    df["athlete_id"] = df["athlete_id"].astype("int64")
    data_rank["athlete_id"] = data_rank["athlete_id"].astype("int64")
    df = add_latest_tuesday(df,'start_date')


    # 1) Athlete’s ranking (on-or-before latest_tuesday)
    df = attach_rank_simple(
        df, data_rank,
        id_col="athlete_id",
        date_col="latest_tuesday",
        rank_date_col="date",
        rank_col="world_ranking",
        out_col="athlete_world_ranking",  # new column in df
        strictly_before=False
    )

    # 2) Opponent’s ranking
    #    Rename columns in a *copy* of data_rank so they align with df's opponent_id
    dr_opp = data_rank.rename(columns={
        "athlete_id": "opponent_id",
        "date": "opponent_date"  # lets us get a separate *_matched column
    })

    df = attach_rank_simple(
        df, dr_opp,
        id_col="opponent_id",  # joins on df.opponent_id
        date_col="latest_tuesday",
        rank_date_col="opponent_date",  # so the matched date column is opponent_date_matched
        rank_col="world_ranking",
        out_col="opponent_world_ranking",
        strictly_before=False
    )
    #df = add_match_rankings(df, data_rank)

    final_cols = [
        'tournament_id', 'tournament_name', 'tournament_grade', 'round_name',
        'athlete_id', 'athlete_name', 'opponent_id', 'opponent_name',
        'win_flag', 'start_date','year',
        'athlete_world_ranking', 'opponent_world_ranking'
    ]

    return df[final_cols]


def add_notable_wins_and_losses(df):
    """
    Adds 'notable_win' and 'lost_to' columns to the DataFrame.

    Notable Win: Athlete wins against a better-ranked opponent
                 (numerically smaller rank value is better).
    Lost To: Athlete loses to any opponent.

    Parameters:
        df (pd.DataFrame): DataFrame with required columns:
            'win_flag', 'athlete_world_ranking', 'opponent_world_ranking',
            'opponent_name'

    Returns:
        pd.DataFrame: Updated DataFrame with new columns.
    """

    def format_name_rank(name, rank):
        return f"{name} ({rank})"

    df = df.drop_duplicates()
    # Notable wins
    df['notable_win'] = df.apply(
        lambda row: format_name_rank(row['opponent_name'], row['opponent_world_ranking'])
        if (row['win_flag'] == 'Won') and (row['athlete_world_ranking'] > row['opponent_world_ranking'])
        else '-',
        axis=1
    )

    # Lost to
    df['lost_to'] = df.apply(
        lambda row: format_name_rank(row['opponent_name'], row['opponent_world_ranking'])
        if row['win_flag'] == 'Lost'
        else 'Won',
        axis=1
    )

    return df

"""
Doubles Notable wins
"""


def add_doubles_win_flag_and_team_names(df):
    # Allowed athlete pairs (order doesn't matter)
    allowed_pairs = {
        frozenset((72435, 70500)),  # Satwiksairaj & Chirag
        frozenset((71612, 59966)),  # Treesa & Gayatri
        frozenset((69560, 98187)),  # Hariharan & Ruban
        frozenset((57372, 94165))  # Dhruv & Tanisha
    }

    def process_row(row):
        team1_pair = frozenset((row['team_1_player_1_id'], row['team_1_player_2_id']))
        team2_pair = frozenset((row['team_2_player_1_id'], row['team_2_player_2_id']))

        # Only restrict athlete's team name
        team1_name = f"{row['team_1_player_1_name']} & {row['team_1_player_2_name']}" if team1_pair in allowed_pairs else None
        team2_name = f"{row['team_2_player_1_name']} & {row['team_2_player_2_name']}" if team2_pair in allowed_pairs else None

        # Opponent names always visible
        team1_name_full = f"{row['team_1_player_1_name']} & {row['team_1_player_2_name']}"
        team2_name_full = f"{row['team_2_player_1_name']} & {row['team_2_player_2_name']}"

        if row['athlete_id'] in (row['team_1_player_1_id'], row['team_1_player_2_id']):
            return pd.Series([
                'Won' if row['winner'] == 1 else 'Lost',
                team1_name,
                row['team_1_id'] if team1_pair in allowed_pairs else None,
                team2_name_full,
                row['team_2_id']
            ])
        elif row['athlete_id'] in (row['team_2_player_1_id'], row['team_2_player_2_id']):
            return pd.Series([
                'Won' if row['winner'] == 2 else 'Lost',
                team2_name,
                row['team_2_id'] if team2_pair in allowed_pairs else None,
                team1_name_full,
                row['team_1_id']
            ])
        else:
            return pd.Series([None, None, None, None, None])

    df[['win_flag', 'athlete_team_name', 'athlete_team_id',
        'opponent_team_name', 'opponent_team_id']] = df.apply(process_row, axis=1)

    # Drop rows where athlete_team_id is missing
    df = df.dropna(subset=['athlete_team_id'])
    # Keep only required columns
    final_cols = [
        'athlete_id', 'tournament_id', 'tournament_name', 'tournament_grade',
        'round_name', 'athlete_team_name', 'athlete_team_id',
        'opponent_team_name', 'opponent_team_id', 'win_flag',
        'start_date'
    ]
    return df[final_cols]

def add_doubles_date_features(df, date_col='start_date'):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df['week_number'] = df[date_col].dt.isocalendar().week
    df['year'] = df[date_col].dt.year
    return df


# ----------------------------
# Step 5: Pipeline Execution
# ----------------------------
def attach_rank_simple_doubles(
    df,
    data_rank,
    id_col="athlete_team_id",
    date_col="latest_tuesday",   # in df
    rank_date_col="date",        # in data_rank
    rank_col="world_ranking",    # in data_rank
    out_col="world_ranking",
    strictly_before=False        # False => on-or-before; True => strictly before
):
    # Minimal copies
    left = df[[id_col, date_col]].copy()
    right = data_rank[[id_col, rank_date_col, rank_col]].copy()

    # Parse datetimes
    left[date_col] = pd.to_datetime(left[date_col], errors="coerce")
    right[rank_date_col] = pd.to_datetime(right[rank_date_col], errors="coerce")

    # Unify ID dtype
    try:
        left[id_col] = left[id_col].astype("Int64")
        right[id_col] = right[id_col].astype("Int64")
    except Exception:
        left[id_col] = left[id_col].astype(str)
        right[id_col] = right[id_col].astype(str)

    # Prepare query rows
    A = left.rename(columns={date_col: "__date__"}).copy()
    A["_q"] = True
    A[rank_col] = pd.NA              # placeholder
    A["__rank_date__"] = pd.NaT

    # Prepare rank rows
    B = right.rename(columns={rank_date_col: "__date__"}).copy()
    B["_q"] = False
    B["__rank_date__"] = B["__date__"]

    # Same-day behavior via sort order
    if strictly_before:
        A["_ord"] = 0; B["_ord"] = 1   # skip same-day
    else:
        A["_ord"] = 1; B["_ord"] = 0   # include same-day

    C = pd.concat([A, B], ignore_index=True, sort=False)
    C = C.sort_values([id_col, "__date__", "_ord"])

    # Avoid name collision if out_col == rank_col
    out_value_col = out_col if out_col != rank_col else "__out_value__"

    # Forward-fill within athlete
    C[out_value_col] = C.groupby(id_col)[rank_col].ffill()
    C["__rank_date__"] = C.groupby(id_col)["__rank_date__"].ffill()

    # Keep only query rows and rename
    drop_cols = ["_q", "_ord"]
    # Only drop rank_col if it's different from out_value_col
    if rank_col != out_value_col:
        drop_cols.append(rank_col)

    out = (
        C[C["_q"]]
        .drop(columns=drop_cols)
        .rename(columns={
            "__date__": date_col,
            "__rank_date__": f"{rank_date_col}_matched",
            out_value_col: out_col
        })
    )

    # Merge back to original df
    return df.merge(
        out[[id_col, date_col, out_col, f"{rank_date_col}_matched"]],
        on=[id_col, date_col],
        how="left"
    )


def process_doubles_notable_wins():
    data = pd.read_sql_query(read_doubles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_doubles_ranking_table(), con=sai_db_engine)

    df = add_doubles_win_flag_and_team_names(data)
    df = add_doubles_date_features(df, date_col='start_date')

    # Ensure data_rank dates are parsed
    data_rank = data_rank.copy()
    data_rank['date'] = pd.to_datetime(data_rank['date'], errors='coerce')

    data_rank = data_rank.rename(columns={"team_id": "athlete_team_id"})
    df = add_latest_tuesday(df, 'start_date')

    df = attach_rank_simple_doubles(
        df, data_rank,
        id_col='athlete_team_id',
        date_col="latest_tuesday",
        rank_date_col="date",
        rank_col="world_ranking",
        out_col="athlete_team_world_ranking",  # new column in df
        strictly_before=False
    )
    # 2) Opponent’s ranking
    #    Rename columns in a *copy* of data_rank so they align with df's opponent_id
    dr_opp = data_rank.rename(columns={
        "athlete_team_id": "opponent_team_id",
        "date": "opponent_team_date"  # lets us get a separate *_matched column
    })

    df = attach_rank_simple_doubles(
        df, dr_opp,
        id_col="opponent_team_id",  # joins on df.opponent_id
        date_col="latest_tuesday",
        rank_date_col="opponent_team_date",  # so the matched date column is opponent_date_matched
        rank_col="world_ranking",
        out_col="opponent_team_world_ranking",
        strictly_before=False
    )

    final_cols = [
        'athlete_id',
        'tournament_id',
        'tournament_name',
        'tournament_grade',
        'round_name',
        'athlete_team_name',
        'athlete_team_id',
        'opponent_team_name',
        'opponent_team_id',
        'win_flag',
        'start_date',
        'year',
        'athlete_team_world_ranking',
        'opponent_team_world_ranking'
    ]
    df = df[final_cols]
    return df

def add_notable_wins_and_losses_doubles(df):
    """
    Adds 'notable_win' and 'lost_to' columns to the DataFrame.

    Notable Win: Athlete wins against a better-ranked opponent
                 (numerically smaller rank value is better).
    Lost To: Athlete loses to any opponent.

    Parameters:
        df (pd.DataFrame): DataFrame with required columns:
            'win_flag', 'athlete_world_ranking', 'opponent_world_ranking',
            'opponent_name'

    Returns:
        pd.DataFrame: Updated DataFrame with new columns.
    """

    def format_name_rank(name, rank):
        return f"{name} ({rank})"

    df = df.drop_duplicates()
    # Notable wins
    df['notable_win'] = df.apply(
        lambda row: format_name_rank(row['opponent_team_name'], row['opponent_team_world_ranking'])
        if (row['win_flag'] == 'Won') and (row['athlete_team_world_ranking'] > row['opponent_team_world_ranking'])
        else '-',
        axis=1
    )

    # Lost to
    df['lost_to'] = df.apply(
        lambda row: format_name_rank(row['opponent_team_name'], row['opponent_team_world_ranking'])
        if row['win_flag'] == 'Lost'
        else 'Won',
        axis=1
    )

    return df









