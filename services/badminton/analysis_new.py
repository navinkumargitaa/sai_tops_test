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






def prepare_athlete_match_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process match-level data to generate athlete-level win/loss, opponent info,
    and reorder columns for final output.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing match data with player IDs, names, and results.

    Returns
    -------
    pd.DataFrame
        Processed DataFrame containing athlete-level information such as win/loss,
        opponent details, and standardized columns.
    """

    # Create Win/Loss flag
    df['win_flag'] = df.apply(
        lambda row: 'Win' if (
            (row['athlete_id'] == row['team_1_player_1_id'] and row['winner'] == 1)
            or (row['athlete_id'] == row['team_2_player_1_id'] and row['winner'] == 2)
        ) else 'Loss',
        axis=1
    )

    # Determine Athlete Name
    df['athlete_name'] = np.where(
        df['athlete_id'] == df['team_1_player_1_id'],
        df['team_1_player_1_name'],
        df['team_2_player_1_name']
    )

    # Determine Opponent ID
    df['opponent_id'] = np.where(
        df['athlete_id'] == df['team_1_player_1_id'],
        df['team_2_player_1_id'],
        df['team_1_player_1_id']
    )
    df['opponent_id'] = pd.to_numeric(df['opponent_id'], errors='coerce').astype('Int64')

    # Determine Opponent Name
    df['opponent_name'] = np.where(
        df['athlete_id'] == df['team_1_player_1_id'],
        df['team_2_player_1_name'],
        df['team_1_player_1_name']
    )

    # Drop unwanted columns (if present)
    drop_cols = [
        'draw_name_full', 'winner',
        'team_1_player_1_id', 'team_1_player_1_name',
        'team_2_player_1_id', 'team_2_player_1_name',
        'date', 'year', 'Unnamed: 0'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')

    # Final column order
    final_order = [
        'tournament_id', 'tournament_match_id', 'tournament_name',
        'tournament_grade', 'round_name', 'athlete_id', 'athlete_name',
        'opponent_id', 'opponent_name', 'win_flag', 'start_date'
    ]
    df = df[[col for col in final_order if col in df.columns]]

    return df


def merge_athlete_ranks(
    df: pd.DataFrame,
    df2: pd.DataFrame,
    start_date_col: str = "start_date"
) -> pd.DataFrame:
    """
    Adjust match start dates to the nearest Tuesday and merge athlete ranking data.

    This function:
      1. Converts start dates to the nearest Tuesday.
      2. Reads and cleans the athlete ranking dataset.
      3. Merges both athlete and opponent world rankings into the match dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Match dataset containing athlete and opponent IDs with start dates.
    rank_file : str, optional
        Path to the athlete rank CSV file (default is "athlete_rank.csv").
    start_date_col : str, optional
        Column name representing the match start date (default is "start_date").

    Returns
    -------
    pd.DataFrame
    DataFrame with adjusted start dates and merged athlete/opponent ranks.
    """

    # --- Step 1: Convert start_date to nearest Tuesday ---
    df[start_date_col] = pd.to_datetime(df[start_date_col], errors='coerce')
    weekday = df[start_date_col].dt.weekday
    forward = (1 - weekday) % 7
    backward = forward - 7
    days_shift = np.where(forward <= np.abs(backward), forward, backward)
    df[start_date_col] = (
        df[start_date_col] + pd.to_timedelta(days_shift, unit='D')
    ).dt.normalize()

    # --- Step 2: Read and clean athlete ranking dataset ---
    rank_df = df2
    rank_df = rank_df.drop(columns=['Unnamed: 0'], errors='ignore')
    rank_df['date'] = pd.to_datetime(rank_df['date'], errors='coerce').dt.normalize()

    rank_lookup = rank_df[['athlete_id', 'date', 'world_ranking']].rename(
        columns={
            'athlete_id': 'player_id',
            'date': 'tuesday_date',
            'world_ranking': 'ranking'
        }
    )

    # --- Step 3: Merge athlete rank ---
    df = df.merge(
        rank_lookup,
        how='left',
        left_on=['athlete_id', start_date_col],
        right_on=['player_id', 'tuesday_date']
    ).rename(columns={'ranking': 'athlete_rank'})

    df = df.drop(columns=[c for c in ['player_id', 'tuesday_date'] if c in df.columns])

    # --- Step 4: Merge opponent rank ---
    df = df.merge(
        rank_lookup,
        how='left',
        left_on=['opponent_id', start_date_col],
        right_on=['player_id', 'tuesday_date']
    ).rename(columns={'ranking': 'opponent_rank'})

    df = df.drop(columns=[c for c in ['player_id', 'tuesday_date'] if c in df.columns])

    # --- Step 5: Final clean-up ---
    df[start_date_col] = df[start_date_col].dt.date
    df['athlete_rank'] = pd.to_numeric(df['athlete_rank'], errors='coerce').astype('Int64')
    df['opponent_rank'] = pd.to_numeric(df['opponent_rank'], errors='coerce').astype('Int64')

    return df


def compute_notable_win(row: pd.Series) -> str:
    """
    Determine if a match is a 'notable win' based on athlete and opponent ranks.

    A 'notable win' is defined as a match where:
      - The athlete won ('win_flag' == 'Win'), and
      - The athlete's rank number is higher (worse) than the opponent's rank number.

    Parameters
    ----------
    row : pd.Series
        A row from the match DataFrame.

    Returns
    -------
    str
        The opponent's name and rank in the format "<Opponent Name> - <Opponent Rank>"
        if it is a notable win, otherwise '--'.
    """
    if row.get("win_flag") != "Win":
        return "--"

    athlete_rank = row.get("athlete_rank")
    opponent_rank = row.get("opponent_rank")

    if pd.isna(athlete_rank) or pd.isna(opponent_rank):
        return "--"

    try:
        if athlete_rank > opponent_rank:
            return f"{row.get('opponent_name', '')} - {int(opponent_rank)}"
        return "--"
    except Exception:
        return np.nan




def add_notable_win_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'notable_win' column and consolidates data into one row per athlete per tournament,
    while retaining all columns from the original DataFrame.

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with notable wins, lost to, and all other columns retained.
    """

    def format_name_rank(name, rank):
        return f"{name} ({rank})" if pd.notna(rank) else str(name)

    # Step 1: Compute notable wins
    df["notable_win"] = df.apply(compute_notable_win, axis=1)

    # Step 2: Ensure datetime
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["year"] = df["start_date"].dt.year

    # Step 3: Lost to logic
    df["lost_to"] = df.apply(
        lambda row: format_name_rank(row["opponent_name"], row["opponent_rank"])
        if row["win_flag"] == "Loss"
        else "Won",
        axis=1,
    )

    # Step 4: Mark champions
    df.loc[
        (df["round_name"] == "Final")
        & (df["win_flag"] == "Win")
        & (df["lost_to"] == "Won"),
        "lost_to",
    ] = "1st Position"

    # Step 5: Normalize round names
    position_map = {
        "R32": "R32",
        "Qual. QF": "<R32",
        "QF": "QF",
        "R64": "<R32",
        "R16": "R16",
        "SF": "SF",
        "R128": "<R32",
        "R2": "<R32",
        "R3": "<R32",
        "R1": "<R32",
        "Final": "F",
        "Qual. R16": "<R32",
        "Qual. R32": "<R32",
        "Qual. R64": "<R32",
        "R5": "R16",
        "3/4": "SF",
        "Semi-finals": "SF",
        "Quarterfinals": "QF",
        "Round of 16": "R16",
    }
    df["round_name"] = df["round_name"].replace(position_map)

    # Step 6: Round order
    round_order = {"<R32": 1, "R32": 2, "R16": 3, "QF": 4, "SF": 5, "F": 6}
    df["round_number"] = df["round_name"].map(round_order).astype("Int64")

    # Step 7: Sort
    df = df.sort_values(by=["athlete_id", "tournament_id", "round_number"])

    # Step 8: Aggregate (combine Lost To, Notable Wins) but retain all other cols
    agg_dict = {col: "last" for col in df.columns if col not in ["lost_to", "notable_win"]}

    agg_dict.update({
        "lost_to": lambda x: (
            "1st Position"
            if "1st Position" in x.values
            else ", ".join(
                sorted(
                    set(
                        v
                        for v in x
                        if isinstance(v, str)
                        and v not in ["Won", "1st Position", "--"]
                    )
                )
            )
            or "--"
        ),
        "notable_win": lambda x: ", ".join(
            sorted(
                set(
                    v
                    for v in x
                    if isinstance(v, str)
                    and v.strip() != ""
                    and v != "--"
                )
            )
        ),
    })

    latest_round_df = (
        df.groupby(["athlete_id", "tournament_id"], as_index=False)
        .agg(agg_dict)
        .rename(columns={"athlete_rank": "athlete_world_ranking",'opponent_rank':'opponent_world_ranking'})
    )
    latest_round_df = latest_round_df.drop(['tournament_match_id', 'round_number'], axis=1)
    return latest_round_df




def merge_athlete_ranks_doubles(
    df: pd.DataFrame,
    df2: pd.DataFrame,
    start_date_col: str = "start_date"
) -> pd.DataFrame:
    """
    Adjust match start dates to the nearest Tuesday and merge athlete ranking data.

    This function:
      1. Converts start dates to the nearest Tuesday.
      2. Reads and cleans the athlete ranking dataset.
      3. Merges both athlete and opponent world rankings into the match dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Match dataset containing athlete and opponent IDs with start dates.
    rank_file : str, optional
        Path to the athlete rank CSV file (default is "athlete_rank.csv").
    start_date_col : str, optional
        Column name representing the match start date (default is "start_date").

    Returns
    -------
    pd.DataFrame
    DataFrame with adjusted start dates and merged athlete/opponent ranks.
    """

    # --- Step 1: Convert start_date to nearest Tuesday ---
    df[start_date_col] = pd.to_datetime(df[start_date_col], errors='coerce')
    weekday = df[start_date_col].dt.weekday
    forward = (1 - weekday) % 7
    backward = forward - 7
    days_shift = np.where(forward <= np.abs(backward), forward, backward)
    df[start_date_col] = (
        df[start_date_col] + pd.to_timedelta(days_shift, unit='D')
    ).dt.normalize()

    # --- Step 2: Read and clean athlete ranking dataset ---
    rank_df = df2
    rank_df = rank_df.drop(columns=['Unnamed: 0'], errors='ignore')
    rank_df['date'] = pd.to_datetime(rank_df['date'], errors='coerce').dt.normalize()

    rank_lookup = rank_df[['team_id', 'date', 'world_ranking']].rename(
        columns={
            'team_id': 'player_id',
            'date': 'tuesday_date',
            'world_ranking': 'ranking'
        }
    )

    # --- Step 3: Merge athlete rank ---
    df = df.merge(
        rank_lookup,
        how='left',
        left_on=['athlete_team_id', start_date_col],
        right_on=['player_id', 'tuesday_date']
    ).rename(columns={'ranking': 'athlete_team_rank'})

    df = df.drop(columns=[c for c in ['player_id', 'tuesday_date'] if c in df.columns])

    # --- Step 4: Merge opponent rank ---
    df = df.merge(
        rank_lookup,
        how='left',
        left_on=['opponent_team_id', start_date_col],
        right_on=['player_id', 'tuesday_date']
    ).rename(columns={'ranking': 'opponent_team_rank'})

    df = df.drop(columns=[c for c in ['player_id', 'tuesday_date'] if c in df.columns])

    # --- Step 5: Final clean-up ---
    df[start_date_col] = df[start_date_col].dt.date
    df['athlete_team_rank'] = pd.to_numeric(df['athlete_team_rank'], errors='coerce').astype('Int64')
    df['opponent_team_rank'] = pd.to_numeric(df['opponent_team_rank'], errors='coerce').astype('Int64')

    return df

def process_singles_notable_wins():
    data = pd.read_sql_query(read_singles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_singles_ranking_table(), con=sai_db_engine)

    df = prepare_athlete_match_data(data)
    df = merge_athlete_ranks(df,data_rank)


    final_df = add_notable_win_column(df)
    # Step 4: Ensure athlete_name is constant for each athlete_id
    name_map = (
        final_df.groupby('athlete_id')['athlete_name']
        .agg(lambda x: x.mode().iat[0] if not x.mode().empty else x.iloc[0])
        .to_dict()
    )
    final_df['athlete_name'] = final_df['athlete_id'].map(name_map)
    final_df['start_date'] = pd.to_datetime(final_df['start_date'])
    final_df = final_df.sort_values(by='start_date', ascending=False)
    name_map = {
        'H. S. PRANNOY': 'PRANNOY H. S.',
        'V. Sindhu PUSARLA': 'PUSARLA V. Sindhu'
    }

    final_df['athlete_name'] = final_df['athlete_name'].replace(name_map)

    return final_df


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

def add_notable_win_column_doubles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'notable_win' column and consolidates data into one row per athlete per tournament,
    while retaining all columns from the original DataFrame.

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with notable wins, lost to, and all other columns retained.
    """

    def format_name_rank(name, rank):
        return f"{name} ({rank})" if pd.notna(rank) else str(name)

    # Step 1: Compute notable wins
    df["notable_win"] = df.apply(compute_notable_win, axis=1)

    # Step 2: Ensure datetime
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["year"] = df["start_date"].dt.year

    # Step 3: Lost to logic
    df["lost_to"] = df.apply(
        lambda row: format_name_rank(row["opponent_team_name"], row["opponent_team_rank"])
        if row["win_flag"] == "Lost"
        else "Won",
        axis=1,
    )

    # Step 4: Mark champions
    df.loc[
        (df["round_name"] == "Final")
        & (df["win_flag"] == "Win")
        & (df["lost_to"] == "Won"),
        "lost_to",
    ] = "1st Position"

    # Step 5: Normalize round names
    position_map = {
        "R32": "R32",
        "Qual. QF": "<R32",
        "QF": "QF",
        "R64": "<R32",
        "R16": "R16",
        "SF": "SF",
        "R128": "<R32",
        "R2": "<R32",
        "R3": "<R32",
        "R1": "<R32",
        "Final": "F",
        "Qual. R16": "<R32",
        "Qual. R32": "<R32",
        "Qual. R64": "<R32",
        "R5": "R16",
        "3/4": "SF",
        "Semi-finals": "SF",
        "Quarterfinals": "QF",
        "Round of 16": "R16",
    }
    df["round_name"] = df["round_name"].replace(position_map)

    # Step 6: Round order
    round_order = {"<R32": 1, "R32": 2, "R16": 3, "QF": 4, "SF": 5, "F": 6}
    df["round_number"] = df["round_name"].map(round_order).astype("Int64")

    # Step 7: Sort
    df = df.sort_values(by=["athlete_id", "tournament_id", "round_number"])

    # Step 8: Aggregate (combine Lost To, Notable Wins) but retain all other cols
    agg_dict = {col: "last" for col in df.columns if col not in ["lost_to", "notable_win"]}

    agg_dict.update({
        "lost_to": lambda x: (
            "1st Position"
            if "1st Position" in x.values
            else ", ".join(
                sorted(
                    set(
                        v
                        for v in x
                        if isinstance(v, str)
                        and v not in ["Won", "1st Position", "--"]
                    )
                )
            )
            or "--"
        ),
        "notable_win": lambda x: ", ".join(
            sorted(
                set(
                    v
                    for v in x
                    if isinstance(v, str)
                    and v.strip() != ""
                    and v != "--"
                )
            )
        ),
    })

    latest_round_df = (
        df.groupby(["athlete_id", "tournament_id"], as_index=False)
        .agg(agg_dict)
        .rename(columns={"athlete_team_rank": "athlete_team_world_ranking",'opponent_team_rank':'opponent_team_world_ranking'})
    )
    latest_round_df = latest_round_df.drop(['round_number'], axis=1)
    return latest_round_df


def process_doubles_notable_wins():
    data = pd.read_sql_query(read_doubles_notable_wins(), con=sai_db_engine)
    data_rank = pd.read_sql_query(read_doubles_ranking_table(), con=sai_db_engine)

    df = add_doubles_win_flag_and_team_names(data)
    df = merge_athlete_ranks_doubles(df, data_rank)
    final_df = add_notable_win_column_doubles(df)
    final_df['start_date'] = pd.to_datetime(final_df['start_date'])
    final_df = final_df.sort_values(by='start_date', ascending=False)
    return final_df

def clean_and_transform_data():
    """Map positions, clean data, and create pivot table."""
    position_map = {
        "R32": "R32", "R16": "R16", "QF": "QF", "N/A": "Team", "1st": "F", "3/4": "SF",
        "3rd": "SF", "2nd": "F", "Qual. R16": "<R32", "R64": "<R32", "Qual. QF": "<R32",
        "Qual. R32": "<R32", "-": None, "R128": "<R32", "Round of 16": "R16", "R3": "<R32",
        "Qual. R128": "<R32", "Qual. R64": "<R32", "R4": "R32", "Final": "F", "R2": "<R32",
        "SF": "SF", "R5": "R16", "Quarterfinals": "QF", "Group A": "<R32"
    }
    data = process_singles_notable_wins()
    # Replace values
    data['round_name'] = data['round_name'].replace(position_map)
    data['tournament_grade'] = data['tournament_grade'].replace('final', '1000')
    #data = data[(data['athlete_id'] == 68870) & (data['round_name'] == 'F')]

    # Step 4: Ensure athlete_name is constant for each athlete_id
    name_map = (
        data.groupby('athlete_id')['athlete_name']
        .agg(lambda x: x.mode().iat[0] if not x.mode().empty else x.iloc[0])
        .to_dict()
    )
    data['athlete_name'] = data['athlete_id'].map(name_map)

    # Step 5: Aggregate before pivot to avoid duplicates
    data['count'] = 1
    grouped = (
        data.groupby(['athlete_id', 'athlete_name', 'year', 'tournament_grade', 'round_name'], as_index=False)['count']
        .sum()
    )

    # Step 6: Create pivot table
    pivot_df = grouped.pivot_table(
        index=['athlete_id', 'athlete_name', 'year'],
        columns=['tournament_grade', 'round_name'],
        values='count',
        aggfunc='sum',
        fill_value=0
    )

    # Step 7: Flatten MultiIndex columns
    pivot_df.columns = [f"{grade}_{pos}" for grade, pos in pivot_df.columns]

    # Step 8: Reset index and return
    pivot_df = pivot_df.reset_index()

    return pivot_df





def make_static_columns(df, order_grade_levels, order_position_stages):
    """Ensure all grade-position combinations exist and reorder columns."""
    desired_cols = [f"{grade}_{pos}" for grade, pos in itertools.product(order_grade_levels, order_position_stages)]
    for col in desired_cols:
        if col not in df.columns:
            df[col] = 0

    df = df[['athlete_id', 'athlete_name', 'year'] + desired_cols]

    return df



def process_singles_tournament_finishes():
    """Main processing pipeline."""

    pivot_df = clean_and_transform_data()
    order_grade_levels = ["Junior", "G3", "Super 100", "Super 300", "Super 500", "Super 750", "Super 1000", "Grade 1"]
    order_position_stages = ["LT_R32", "R32", "R16", "QF", "SF", "F"]


    final_df = make_static_columns(pivot_df, order_grade_levels, order_position_stages)
    final_df = final_df.rename(columns={'year': 'tournament_year'})
    # final_df = final_df.drop_duplicates(subset=['athlete_id', 'tournament_year'], keep='last')
    name_map = {
        'H. S. PRANNOY': 'PRANNOY H. S.',
        'V. Sindhu PUSARLA': 'PUSARLA V. Sindhu'
    }
    final_df['athlete_name'] = final_df['athlete_name'].replace(name_map)

    return final_df



def clean_and_transform_data_doubles():
    """Map positions, clean data, and create pivot table."""
    position_map = {
        "R32": "R32", "R16": "R16", "QF": "QF", "N/A": "Team", "1st": "F", "3/4": "SF",
        "3rd": "SF", "2nd": "F", "Qual. R16": "<R32", "R64": "<R32", "Qual. QF": "<R32",
        "Qual. R32": "<R32", "-": None, "R128": "<R32", "Round of 16": "R16", "R3": "<R32",
        "Qual. R128": "<R32", "Qual. R64": "<R32", "R4": "R32", "Final": "F", "R2": "<R32",
        "SF": "SF", "R5": "R16", "Quarterfinals": "QF", "Group A": "<R32"
    }
    new_data = process_doubles_notable_wins()
    # Replace values
    new_data['round_name'] = new_data['round_name'].replace(position_map)
    new_data['tournament_grade'] = new_data['tournament_grade'].replace('final', '1000')


    # Create pivot table
    pivot_df = new_data.pivot_table(
        index=['athlete_team_id', 'athlete_team_name', 'year', 'tournament_grade'],
        columns='round_name',
        aggfunc='size',
        fill_value=0
    )

    # Unstack grade level into columns
    pivot_df = pivot_df.unstack(level='tournament_grade', fill_value=0)

    # Flatten MultiIndex columns
    pivot_df.columns = [f"{grade}_{pos}" for pos, grade in pivot_df.columns]

    return pivot_df.reset_index()

def make_static_columns_doubles(df, order_grade_levels, order_position_stages):
    """Ensure all grade-position combinations exist and reorder columns."""
    desired_cols = [f"{grade}_{pos}" for grade, pos in itertools.product(order_grade_levels, order_position_stages)]
    for col in desired_cols:
        if col not in df.columns:
            df[col] = 0

    df = df[['athlete_team_id', 'athlete_team_name', 'year'] + desired_cols]

    return df


def process_doubles_tournament_finishes():
    """Main processing pipeline."""

    pivot_df = clean_and_transform_data_doubles()
    order_grade_levels = ["Junior", "G3", "Super 100", "Super 300", "Super 500", "Super 750", "Super 1000", "Grade 1"]
    order_position_stages = ["LT_R32", "R32", "R16", "QF", "SF", "F"]


    final_df = make_static_columns_doubles(pivot_df, order_grade_levels, order_position_stages)
    final_df = final_df.rename(columns={'year': 'tournament_year'})
    final_df = final_df.rename(columns={'athlete_team_id': 'athlete_id','athlete_team_name':'athlete_name'})
    final_df = final_df.drop_duplicates(subset=['athlete_id', 'tournament_year'], keep='last')

    return final_df

