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
from orm.badminton.doubles_notable_wins import DoublesNotableWinsWithRanks,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import process_doubles_notable_wins,build_notable_wins_doubles_final_table

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
    Base.metadata.create_all(bind=sai_db_engine, tables=[DoublesNotableWinsWithRanks.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    notable_wins_doubles = process_doubles_notable_wins()
    print("✅ Data extracted and transformed:")

    new_df = build_notable_wins_doubles_final_table(notable_wins_doubles)


    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(DoublesNotableWinsWithRanks).delete()
        # session.commit()

        records = [
            DoublesNotableWinsWithRanks(
                tournament_id=int(row["tournament_id"]),
                tournament_name=row["tournament_name"],
                tournament_grade=row["tournament_grade"],
                round_name=row["round_name"],

                athlete_team_name=row["athlete_team_name"],
                athlete_team_id=None if pd.isna(row["athlete_team_id"]) else int(row["athlete_team_id"]),

                opponent_team_name=row["opponent_team_name"],
                opponent_team_id=None if pd.isna(row["opponent_team_id"]) else int(row["opponent_team_id"]),

                win_flag=row["win_flag"],
                start_date=row["start_date"],
                week_number=int(row["week_number"]),
                year=int(row["year"]),

                athlete_team_id_rank=None if pd.isna(row["athlete_team_id_rank"]) else int(row["athlete_team_id_rank"]),
                opponent_team_id_rank=None if pd.isna(row["opponent_team_id_rank"]) else int(
                    row["opponent_team_id_rank"])
            )
            for _, row in notable_wins_doubles.iterrows()
        ]

        # Step 5: Insert records
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into z_badminton_doubles_notable_wins_viz.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()

if __name__ == '__main__':
    main()