




import pandas as pd
from model.archery.database import sai_db_engine, read_archery_ranking,read_archery_comp_ranking
from model.archery.database import read_archery_qual_results,read_archery_elem_results

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


def compute_avg_arrow_score(sp_str):
        if pd.isna(sp_str):
            return None
        try:
            sets = sp_str.split('|')
            total_points = sum(int(s) for s in sets)
            total_arrows = len(sets) * 3  # 3 arrows per set
            return round(total_points / total_arrows, 2)
        except:
            return None



def get_arrow_average():
    """

    :return:
    """
    query_qual = read_archery_qual_results()
    query_elem = read_archery_elem_results()

    qual_data = pd.read_sql_query(query_qual,con=sai_db_engine)
    elem_data = pd.read_sql_query(query_elem,con=sai_db_engine)

    # Make sure date_from is a datetime object
    qual_data['comp_date'] = pd.to_datetime(qual_data['comp_date'])
    # Create the new column combining place and year
    qual_data['comp_new_short_name'] = qual_data['comp_place'] + ' ' + qual_data['comp_date'].dt.year.astype(str)

    elem_data['comp_date'] = pd.to_datetime(elem_data['comp_date'])
    # Create the new column combining place and year
    elem_data['comp_new_short_name'] = elem_data['comp_place'] + ' ' + elem_data['comp_date'].dt.year.astype(str)

    qual_data['avg_arrow_score_qualification'] = round(qual_data['qual_score'] / 72, 2)

    # Apply to the DataFrame
    elem_data['avg_arrow_score_elemination_round'] = elem_data['set_points'].apply(compute_avg_arrow_score)

    # Ensure relevant columns are numeric
    elem_data["avg_arrow_score_elemination_round"] = pd.to_numeric(
        elem_data["avg_arrow_score_elemination_round"], errors="coerce"
    )
    qual_data["avg_arrow_score_qualification"] = pd.to_numeric(
        qual_data["avg_arrow_score_qualification"], errors="coerce"
    )

    # Step 1: Compute elimination round average score per athlete per competition
    elim_avg = (
        elem_data.groupby(["athlete_id", "athlete_name", "competition_id", "comp_new_short_name"])[
            "avg_arrow_score_elemination_round"]
        .mean()
        .reset_index()
        .rename(columns={"avg_arrow_score_elemination_round": "elem_avg_arrow"})
    )

    # Step 2: Extract qualification round average
    qual_avg = qual_data[[
        "athlete_id", "athlete_name", "competition_id", "comp_new_short_name", "avg_arrow_score_qualification"
    ]].rename(columns={"avg_arrow_score_qualification": "qual_avg_arrow"})

    # Step 3: Merge the two on athlete and competition
    merged = pd.merge(elim_avg, qual_avg, on=["athlete_id", "athlete_name", "competition_id", "comp_new_short_name"],
                      how="outer")

    # Step 4: Calculate overall competition average arrow score
    merged["competition_avg_arrow"] = merged[["qual_avg_arrow", "elem_avg_arrow"]].mean(axis=1)

    # Step 5: Final column ordering
    final_df = merged[[
        "athlete_id", "athlete_name", "competition_id", "comp_new_short_name",
        "qual_avg_arrow", "elem_avg_arrow", "competition_avg_arrow"
    ]]

    return final_df
