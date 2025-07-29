"""
main.py

Main entry point for loading badminton singles ranking
into the 'badminton_singles_ranking_viz' table.

"""

__author__ = "navin@gitaa.in"

# Importing required modules
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.badminton.doubles_ranking import BadmintonDoublesRanking,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import get_doubles_ranking_data

def main():
    """
    Orchestrates the ETL process:
    - Creates the database table if it doesn't exist
    - Extracts and transforms athlete ranking data
    - Loads the processed data into the target table
    """

    # Step 1: Ensure the table exists in the database
    Base.metadata.create_all(sai_db_engine)
    print("Table created successfully (or already exists).")

    # Step 2: Force table creation
    Base.metadata.create_all(bind=sai_db_engine, tables=[BadmintonDoublesRanking.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    doubles_ranking = get_doubles_ranking_data()
    print("Data extracted and transformed:")

    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(BadmintonSinglesRanking).delete()
        # session.commit()

        records = [
            BadmintonDoublesRanking(
                athlete_1_id=int(row['athlete_1_id']) if pd.notnull(row['athlete_1_id']) else None,
                athlete_1_name=row['athlete_1_name'] if pd.notnull(row['athlete_1_name']) else None,
                athlete_2_id=int(row['athlete_2_id']) if pd.notnull(row['athlete_2_id']) else None,
                athlete_2_name=row['athlete_2_name'] if pd.notnull(row['athlete_2_name']) else None,
                composite_team_id=row['composite_team_id'] if pd.notnull(row['composite_team_id']) else None,
                team_display_name=row['team_display_name'] if pd.notnull(row['team_display_name']) else None,
                ranking_date=row['ranking_date'].date() if pd.notnull(row['ranking_date']) else None,
                ranking_year=int(row['ranking_date'].year) if pd.notnull(row['ranking_date']) else None,
                world_ranking=int(row['world_ranking']) if pd.notnull(row['world_ranking']) else None,
                world_tour_ranking=int(row['world_tour_ranking']) if pd.notnull(row['world_tour_ranking']) else None
            )
            for _, row in doubles_ranking.iterrows()
        ]

        session.add_all(records)
        session.commit()
        print(f"Loaded {len(records)} records into the badminton_doubles_ranking_viz table.")

    except Exception as e:
        session.rollback()
        print("Commit failed:")
        print(e)

    finally:
        session.close()

if __name__ == '__main__':
    main()