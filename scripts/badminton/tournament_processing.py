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
from orm.badminton.tournament_processing import BadmintonTournamentProcessing,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import process_tournament_grade


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
    Base.metadata.create_all(bind=sai_db_engine, tables=[BadmintonTournamentProcessing.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    tournament_processing = process_tournament_grade()
    print("✅ Data extracted and transformed:")


    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(BadmintonTournamentProcessing).delete()
        # session.commit()

        # Step 4: Convert each row to an ORM object
        records = [
            BadmintonTournamentProcessing(
                tournament_id=int(row['tournament_id']),
                tournament_name=row['name'],
                grade=row['grade'],
                new_grade=row['new_grade'],
                date=row['date'],
                year=int(row['year']) if pd.notnull(row['year']) else None,
                start_date=row['start_date'] if pd.notnull(row['start_date']) else None,
                end_date=row['end_date'] if pd.notnull(row['end_date']) else None,
                athlete_id=int(row['athlete_id']) if pd.notnull(row['athlete_id']) else None
            )
            for _, row in tournament_processing.iterrows()
        ]

        # Step 5: Insert records
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into the badminton_tournament_details_viz table.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()

if __name__ == '__main__':
    main()



