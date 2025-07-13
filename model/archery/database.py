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

def read_archery_qual_results():
    """

    :return:
    """
    athlete_ids = [8734, 5354, 42056, 22125, 39665, 39157, 17262, 43889, 45025, 31020, 17287, 45024, 27737, 27738]

    # Convert the list of IDs into a comma-separated string
    id_list_str = ", ".join(map(str, athlete_ids))

    query=f"""
    SELECT
          a.competition_id AS competition_id,
          a.athlete_id AS athlete_id,
          a.name as athlete_name,
          a.rank as qual_rank,
          a.gold as qual_gold,
          a.x_nine as qual_x_nine,
          a.score AS qual_score,
          b.name_short AS comp_name,
          b.name as comp_full_name,
          b.place as comp_place,
          b.level as comp_level,
          b.sub_level as comp_sub_level,
          b.date_from AS comp_date
        FROM sai.archery_competition_ind_qual_result a
        JOIN sai.archery_competition b
          ON a.competition_id = b.competition_id
        WHERE a.athlete_id IN ({id_list_str});
    """

    return query

def read_archery_elem_results():
    """

    :return:
    """
    athlete_ids = [8734, 5354, 42056, 22125, 39665, 39157, 17262, 43889, 45025, 31020, 17287, 45024, 27737, 27738]

    # Convert the list of IDs into a comma-separated string - yes
    id_list_str = ", ".join(map(str, athlete_ids))

    query = f"""
            SELECT
              a.competition_id AS competition_id,
              a.athlete_id AS athlete_id,
              a.athlete_name AS athlete_name,
              a.qual_rank as qual_rank,
              c.phase AS match_phase,
              a.arrow AS arrow,
              a.sp AS set_points,
              a.win_lose AS win_lose,
              a.bye AS bye,
              b.name_short AS comp_name,
              b.name AS comp_full_name,
              b.level AS comp_level,
              b.place as comp_place,
              b.sub_level AS comp_sub_level,
              b.date_from AS comp_date
            FROM sai.archery_competition_individual_match_competitor a
            JOIN sai.archery_competition b
              ON a.competition_id = b.competition_id
            LEFT JOIN sai.archery_competition_individual_match c
              ON a.competition_ind_match_id = c.row_id
            WHERE a.athlete_id IN ({id_list_str})
        """

    return query
