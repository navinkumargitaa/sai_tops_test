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
# Step 4: Attach rankings without merge_asof
# ----------------------------
def attach_ranks_without_merge_asof(df, data_rank):
    df = df.copy()
    ranks = data_rank.copy()

    # Normalize dates
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    ranks['date'] = pd.to_datetime(ranks['date'], errors='coerce')

    # Normalize IDs to integer
    df['athlete_id'] = pd.to_numeric(df['athlete_id'], errors='coerce').astype('Int64')
    df['opponent_id'] = pd.to_numeric(df['opponent_id'], errors='coerce').astype('Int64')
    ranks['athlete_id'] = pd.to_numeric(ranks['athlete_id'], errors='coerce').astype('Int64')

    # Prepare ranks: one row per (athlete_id, date), sorted by date
    ranks = (
        ranks[['athlete_id', 'date', 'world_ranking']]
        .dropna(subset=['athlete_id', 'date'])
        .drop_duplicates(subset=['athlete_id', 'date'], keep='last')
        .sort_values(['athlete_id', 'date'], kind='mergesort')
        .reset_index(drop=True)
    )

    # Ensure rankings are integers where possible
    ranks['world_ranking'] = pd.to_numeric(ranks['world_ranking'], errors='coerce').astype('Int64')

    # Pre-split ranks into arrays per athlete for fast lookup
    ranks_by_id = {
        int(aid): (g['date'].to_numpy(), g['world_ranking'].to_numpy(dtype='int64'))
        for aid, g in ranks.groupby('athlete_id', sort=False)
    }

    def asof_lookup(ids: pd.Series, dates: pd.Series, right_map, out_name: str):
        out = np.full(len(ids), np.nan, dtype='float64')
        for k, idx in ids.groupby(ids, sort=False).groups.items():
            if pd.isna(k):
                continue
            rd = right_map.get(int(k))
            if rd is None:
                continue
            r_dates, r_vals = rd
            pos = np.searchsorted(r_dates, dates.iloc[idx].to_numpy(), side='right') - 1
            valid = pos >= 0
            if np.any(valid):
                out_idx = np.asarray(list(idx))[valid]
                out[out_idx] = r_vals[pos[valid]]
        # Convert to nullable integer type (keeps NaN support but shows int)
        return pd.Series(pd.Series(out).astype('Int64'), index=ids.index, name=out_name)

    # Attach athlete and opponent world ranking
    df['athlete_world_ranking'] = asof_lookup(df['athlete_id'], df['start_date'], ranks_by_id, 'athlete_world_ranking')
    df['opponent_world_ranking'] = asof_lookup(df['opponent_id'], df['start_date'], ranks_by_id, 'opponent_world_ranking')

    return df

# ----------------------------
# Step 5: Pipeline Execution
# ----------------------------
def process_singles_notable_wins():
    data = pd.read_sql_query(read_singles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_singles_ranking_table(), con=sai_db_engine)

    df = add_win_flag_and_names(data)
    df = clean_and_reorder_columns(df)
    df = add_date_features(df, date_col='start_date')

    # Ensure data_rank dates are parsed
    data_rank = data_rank.copy()
    data_rank['date'] = pd.to_datetime(data_rank['date'], errors='coerce')

    df = attach_ranks_without_merge_asof(df, data_rank)

    final_cols = [
        'tournament_id', 'tournament_name', 'tournament_grade', 'round_name',
        'athlete_id', 'athlete_name', 'opponent_id', 'opponent_name',
        'win_flag', 'start_date','year',
        'athlete_world_ranking', 'opponent_world_ranking'
    ]
    return df[final_cols]

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


# ---------- Helpers ----------
def _coerce_dates_and_ids(df: pd.DataFrame, ranks: pd.DataFrame, normalize_to_day: bool = True):
    """
    - Parses dates
    - Optionally normalizes to calendar-day (so equality on the same day counts)
    - Aligns team-id dtypes to nullable Int64
    """
    df = df.copy()
    ranks = ranks.copy()

    # Parse to datetime
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    ranks['date']    = pd.to_datetime(ranks['date'], errors='coerce')

    # Normalize to day to ensure 2024-06-11 15:00 == 2024-06-11 00:00
    if normalize_to_day:
        # drop timezone if present, then normalize to midnight
        df['start_date'] = df['start_date'].dt.tz_localize(None).dt.normalize()
        ranks['date']    = ranks['date'].dt.tz_localize(None).dt.normalize()
    else:
        # just drop timezone if present, keep time-of-day
        if getattr(df['start_date'].dt, 'tz', None) is not None:
            df['start_date'] = df['start_date'].dt.tz_localize(None)
        if getattr(ranks['date'].dt, 'tz', None) is not None:
            ranks['date'] = ranks['date'].dt.tz_localize(None)

    # IDs -> nullable Int64 (handles strings/floats/ints; keeps NaN)
    to_int64 = lambda s: pd.to_numeric(s, errors='coerce').astype('Int64')
    df['athlete_team_id']  = to_int64(df.get('athlete_team_id'))
    df['opponent_team_id'] = to_int64(df.get('opponent_team_id'))
    ranks['team_id']       = to_int64(ranks.get('team_id'))

    return df, ranks

def _latest_rank_on_or_before(df_ids: pd.Series,
                              df_dates: pd.Series,
                              ranks_team: pd.DataFrame) -> pd.Series:
    """
    For a single role (athlete/opponent):
    - df_ids: team ids from df (Int64)
    - df_dates: start_date from df (datetime64[ns])
    - ranks_team: DataFrame with columns ['team_id','date','world_ranking']
    Returns a Series aligned to df with the most recent world_ranking where rank_date <= start_date.
    """
    out = pd.Series(index=df_ids.index, dtype='float64')  # keep float to allow NaN

    # Clean and sort ranks by team, then date (stable)
    ranks_team = (
        ranks_team.dropna(subset=['team_id', 'date'])
                  .sort_values(['team_id', 'date'], kind='mergesort')
    )

    # Process each team present in df (vectorized by team groups)
    for team, idx in df_ids.dropna().groupby(df_ids).groups.items():
        # Slice this team's ranking timeline
        r = ranks_team.loc[ranks_team['team_id'] == team, ['date', 'world_ranking']]
        if r.empty:
            continue

        dates = r['date'].to_numpy()
        vals  = r['world_ranking'].to_numpy()

        # Tournament dates for these rows
        td = df_dates.loc[idx].to_numpy()

        # Index of last rank date <= start_date (exact matches included)
        pos = np.searchsorted(dates, td, side='right') - 1

        # Assign only where a prior/same-day snapshot exists
        result = np.full(td.shape, np.nan, dtype='float64')
        valid = pos >= 0
        result[valid] = vals[pos[valid]]

        out.loc[idx] = result

    return out


def attach_team_ranks_without_asof(df: pd.DataFrame, ranks: pd.DataFrame, normalize_to_day: bool = True) -> pd.DataFrame:
    """
    Adds:
      - athlete_team_id_rank
      - opponent_team_id_rank
    computed as the latest available 'world_ranking' from 'ranks'
    on or before each row's 'start_date'. If start_date == rank date, it's used.
    """
    df2, ranks2 = _coerce_dates_and_ids(df, ranks, normalize_to_day=normalize_to_day)

    # Athlete
    df2['athlete_team_id_rank'] = _latest_rank_on_or_before(
        df2['athlete_team_id'],
        df2['start_date'],
        ranks2[['team_id', 'date', 'world_ranking']]
    )

    # Opponent
    df2['opponent_team_id_rank'] = _latest_rank_on_or_before(
        df2['opponent_team_id'],
        df2['start_date'],
        ranks2[['team_id', 'date,', 'world_ranking']].rename(columns={'date,': 'date'})  # safety if a stray comma sneaks in
        if 'date,' in ranks2.columns else ranks2[['team_id', 'date', 'world_ranking']]
    )

    return df2


# ----------------------------
# Step 5: Pipeline Execution
# ----------------------------
def process_doubles_notable_wins():
    data = pd.read_sql_query(read_doubles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_doubles_ranking_table(), con=sai_db_engine)

    df = add_doubles_win_flag_and_team_names(data)
    df = add_doubles_date_features(df, date_col='start_date')

    # Ensure data_rank dates are parsed
    data_rank = data_rank.copy()
    data_rank['date'] = pd.to_datetime(data_rank['date'], errors='coerce')

    df = attach_team_ranks_without_asof(df, data_rank)
    # Convert rank columns to integers (set NaN to None)
    for col in ['athlete_team_id_rank', 'opponent_team_id_rank']:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))

    final_cols = [
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
        'week_number',
        'year',
        'athlete_team_id_rank',
        'opponent_team_id_rank'
    ]
    return df[final_cols]

import pandas as pd
import numpy as np

def build_notable_wins_doubles_final_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns:
    ['tournament_id','tournament_name','tournament_grade','round_name',
     'athlete_team_name','athlete_team_id','opponent_team_name','opponent_team_id',
     'win_flag','start_date','week_number','year','athlete_team_id_rank','opponent_team_id_rank']
    Returns a DataFrame with:
    ['tournament_id','tournament_name','tournament_grade','round_name',
     'athlete_team_name','notabele_win','lost_to']
    """

    # Robust booleans for win_flag (handles 1/0, True/False, 'W'/'L', 'win'/'loss')
    win_str = df['win_flag'].astype(str).str.strip().str.lower()
    win = win_str.isin({'1','true','t','w','win','won','y','yes'})

    # Ensure ranks are numeric for comparison
    a_rank = pd.to_numeric(df['athlete_team_id_rank'], errors='coerce')
    o_rank = pd.to_numeric(df['opponent_team_id_rank'], errors='coerce')

    # Opponent display like "J Christie (2)"
    # If rank is missing, show just the name
    opp_rank_str = np.where(o_rank.notna(), o_rank.astype('Int64').astype(str), None)
    opponent_display = np.where(opp_rank_str == None,
                                df['opponent_team_name'].fillna(''),
                                df['opponent_team_name'].fillna('') + ' (' + opp_rank_str + ')')

    # Notable win = athlete won AND athlete rank < opponent rank
    notable_mask = win & (a_rank < o_rank)

    notabele_win = np.where(notable_mask, opponent_display, '-')

    # lost_to = opponent name (with rank) if athlete lost; else "won"
    lost_to = np.where(win, 'won', opponent_display)

    out = pd.DataFrame({
        'tournament_id': df['tournament_id'],
        'tournament_name': df['tournament_name'],
        'tournament_grade': df['tournament_grade'],
        'round_name': df['round_name'],
        'athlete_team_name': df['athlete_team_name'],
        'notabele_win': notabele_win,
        'lost_to': lost_to
    })

    return out






