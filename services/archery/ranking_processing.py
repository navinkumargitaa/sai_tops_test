




import pandas as pd
from model.archery.database import sai_db_engine, read_archery_ranking,read_archery_comp_ranking

def get_end_of_october_ranking():
    """
    Extract and process archery rankings to get end-of-October snapshot.
    Returns a cleaned pandas DataFrame.
    """
    # Extract from DB
    query = read_archery_ranking()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Transform
    df['rank_date_issued'] = pd.to_datetime(df['rank_date_issued'], errors='coerce')
    df['year'] = df['rank_date_issued'].dt.year
    df['month'] = df['rank_date_issued'].dt.month

    october_df = df[df['month'] == 10]
    end_of_october = (
        october_df
        .sort_values('rank_date_issued')
        .groupby(['athlete_id', 'year'])
        .tail(1)
    )

    end_of_october = end_of_october[['athlete_id', 'year', 'rank', 'points', 'rank_date_issued']]
    return end_of_october


def get_competition_ranking():
    """

    :return:
    """
    query = read_archery_comp_ranking()
    df = pd.read_sql_query(query, con=sai_db_engine)

    # Make sure date_from is a datetime object
    df['comp_date'] = pd.to_datetime(df['comp_date'])

    # Create the new column combining place and year
    df['comp_new_short_name'] = df['comp_place'] + ' ' + df['comp_date'].dt.year.astype(str)

    return df