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
from orm.badminton.singles_notable_wins import NotableWinsWithRanks,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import process_singles_notable_wins

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
    Base.metadata.create_all(bind=sai_db_engine, tables=[NotableWinsWithRanks.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    notable_wins_singles = process_singles_notable_wins()
    print("✅ Data extracted and transformed:")


    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(BadmintonTournamentProcessing).delete()
        # session.commit()

        records = [
            NotableWinsWithRanks(
                tournament_id=int(row["tournament_id"]),
                tournament_name=row["tournament_name"],
                tournament_grade=row["tournament_grade"],
                round_name=row["round_name"],
                athlete_id=None if pd.isna(row["athlete_id"]) else int(row["athlete_id"]),
                athlete_name=row["athlete_name"],
                opponent_id=None if pd.isna(row["opponent_id"]) else int(row["opponent_id"]),
                opponent_name=row["opponent_name"],
                win_flag=row["win_flag"],
                start_date=row["start_date"],
                year=int(row["year"]),
                athlete_world_ranking=None if pd.isna(row["athlete_world_ranking"]) else int(
                    row["athlete_world_ranking"]),
                opponent_world_ranking=None if pd.isna(row["opponent_world_ranking"]) else int(
                    row["opponent_world_ranking"])
            )
            for _, row in notable_wins_singles.iterrows()
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