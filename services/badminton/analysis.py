"""
analysis.py

Service functions to extract and process badminton data

"""

__author__ = "navin@gitaa.in"

import pandas as pd
from fuzzywuzzy import process
import calendar
from datetime import date
from model.badminton.database import SessionLocal
from model.badminton.database import read_singles_ranking_progression,read_doubles_ranking_progression,sai_db_engine
from model.badminton.database import read_all_tournament_details



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






