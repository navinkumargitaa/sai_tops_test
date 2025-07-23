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
from orm.badminton.singles_ranking import BadmintonSinglesRanking,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import get_singles_ranking_data

def main():
    """
    Orchestrates the ETL process:
    - Creates the database table if it doesn't exist
    - Extracts and transforms athlete ranking data
    - Loads the processed data into the target table
    """

    # Step 1: Ensure the table exists in the database
    Base.metadata.create_all(sai_db_engine)
    print("✅ Table created successfully (or already exists).")

    # Step 2: Force table creation
    Base.metadata.create_all(bind=sai_db_engine, tables=[BadmintonSinglesRanking.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    singles_ranking = get_singles_ranking_data()
    print("✅ Data extracted and transformed:")

    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(BadmintonSinglesRanking).delete()
        # session.commit()

        # Step 4: Convert each row to an ORM object
        records = [
            BadmintonSinglesRanking(
                athlete_id=int(row['athlete_id']),
                athlete_name=row['athlete_name'],
                ranking_date=row['ranking_date'].date() if pd.notnull(row['ranking_date']) else None,
                ranking_year=int(row['ranking_date'].year) if pd.notnull(row['ranking_date']) else None,
                world_ranking=int(row['world_ranking']) if pd.notnull(row['world_ranking']) else None,
                world_tour_ranking=int(row['world_tour_ranking']) if pd.notnull(row['world_tour_ranking']) else None
            )
            for _, row in singles_ranking.iterrows()
        ]

        # Step 5: Insert records
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into the badminton_singles_ranking_viz table.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()

if __name__ == '__main__':
    main()