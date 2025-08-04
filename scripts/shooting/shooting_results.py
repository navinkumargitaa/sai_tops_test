"""
main.py

Main entry point for loading athlete events

"""

__author__ = "navin@gitaa.in"

# Importing required modules
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.shooting.athlete_shooting_results import ShootingResultsViz, Base

# Import database engine
from model.shooting.database import sai_db_engine

from services.shooting.analysis import load_shooting_results_data,load_all_results_data,prepare_ranked_results,calculate_qmin_qmax,attach_q_min_q_max

def main():
    """
    Orchestrates the ETL process:
    - Creates the required table if it doesn't exist
    - Extracts and transforms athlete arrow average data
    - Loads the transformed data into the database
    """

    # Step 1: Create the table if it does not exist
    Base.metadata.create_all(sai_db_engine)
    print("Table created successfully (or already exists).")

    # Step 2: Force table creation
    Base.metadata.create_all(bind=sai_db_engine, tables=[ShootingResultsViz.__table__])
    print("Table creation attempted.")

    # Step 3: Set up a session for writing data to the database
    Session = sessionmaker(bind=sai_db_engine)  # Bind session to the DB engine
    session = Session()  # Create a new session instance

    # Step 1: Load and process main data
    raw_df = load_shooting_results_data(sai_db_engine)
    merged_df = prepare_ranked_results(raw_df)

    # Step 2: Load and process qmin/qmax data
    qual_df_all = load_all_results_data(sai_db_engine)
    qminmax_df = calculate_qmin_qmax(qual_df_all)

    # Step 3: Merge qmin/qmax into main results
    final_df = attach_q_min_q_max(merged_df, qminmax_df)

    records = [
        ShootingResultsViz(
            competition_id=row.get("competition_id"),
            competition_name=row.get("competition_name"),
            event_name=row.get("event_name"),
            comp_year=row.get("comp_year"),
            comp_date=row.get("comp_date"),
            athlete_name=row.get("athlete_name"),
            score=row.get("score"),
            last_attained_rank=row.get("last_attained_rank"),
            rank_type=row.get("rank_type"),
            host_nation=row.get("host_nation"),
            host_nation_code=row.get("host_nation_code"),
            host_city=row.get("host_city"),
            comp_type=row.get("comp_type"),
            comp_short_name=row.get('comp_short_name'),
            q_min=row.get("q_min"),
            q_max=row.get("q_max")
        )
        for _, row in final_df.iterrows()
    ]

    # Step 5: Add all records to the session
    session.add_all(records)

    # Step 6: Commit the session to persist changes in the database
    session.commit()

    # Step 7: Close the session
    session.close()

    # Step 8: Confirm how many records were loaded
    print(f"Loaded {len(records)} records into the database.")

# Run the ETL process only if this script is executed directly
if __name__ == "__main__":
    main()

