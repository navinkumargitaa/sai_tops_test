"""
This module creates a SQLAlchemy engine for the SAI database
and provides a query function for archery athlete rankings.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine

sai_db_engine = create_engine(url="mysql+pymysql://root:root@localhost:3306/sai")


def read_archery_ranking():
    """
    Returns the SQL query to fetch ranking history for selected archery athletes.
    """
    athlete_ids = [8734, 5354, 42056, 22125, 39665, 39157,17262, 43889, 45025, 31020, 17287, 45024, 27737, 27738]

    # Convert the list of IDs into a comma-separated string
    id_list_str = ", ".join(map(str, athlete_ids))

    query = f"""
    SELECT * FROM sai.archery_athlete_ranking_history
    WHERE athlete_id IN ({id_list_str});
    """

    return query

def read_archery_comp_ranking():
    """
    Returns the SQL query to fetch competition ranking for selected archery athletes.
    :return:
    """
    athlete_ids = [8734, 5354, 42056, 22125, 39665, 39157, 17262, 43889, 45025, 31020, 17287, 45024, 27737, 27738]

    # Convert the list of IDs into a comma-separated string
    id_list_str = ", ".join(map(str, athlete_ids))

    query = f"""
    SELECT
          a.competition_id AS comp_id,
          b.name AS comp_full_name,
          b.name_short AS comp_short_name,
          b.place AS comp_place,
          b.date_from AS comp_date,
          a.athlete_id AS athlete_id,
          a.name AS athlete_name,
          a.rank AS comp_rank
        FROM
          sai.archery_competition_ind_ranking a
        JOIN
          sai.archery_competition b
          ON a.competition_id = b.competition_id
        WHERE
          a.athlete_id IN ({id_list_str});
    """
    return query

