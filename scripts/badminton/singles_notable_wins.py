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
from orm.badminton.singles_notable_wins import NotableWinsSinglesFinal,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import process_singles_notable_wins,build_notable_wins_singles_final_table,add_notable_wins_and_losses

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
    Base.metadata.create_all(bind=sai_db_engine, tables=[NotableWinsSinglesFinal.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    process_notable_wins_singles = process_singles_notable_wins()
    print("✅ Data extracted and transformed:")

    #notable_wins_singles = build_notable_wins_singles_final_table(process_notable_wins_singles)

    notable_wins_singles = add_notable_wins_and_losses(process_notable_wins_singles)
    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(NotableWinsSinglesFinal).delete()
        # session.commit()

        records = [
            NotableWinsSinglesFinal(
                tournament_id=int(row["tournament_id"]) if not pd.isna(row["tournament_id"]) else None,
                tournament_name=row.get("tournament_name"),
                tournament_grade=row.get("tournament_grade"),
                round_name=row.get("round_name"),
                athlete_id=int(row["athlete_id"]) if not pd.isna(row["athlete_id"]) else None,
                athlete_name=row.get("athlete_name"),
                opponent_id=int(row["opponent_id"]) if not pd.isna(row["opponent_id"]) else None,
                opponent_name=row.get("opponent_name"),
                win_flag=row["win_flag"] if not pd.isna(row["win_flag"]) else None,
                start_date=pd.to_datetime(row["start_date"], errors="coerce").date()
                if not pd.isna(row["start_date"]) else None,
                year=int(row["year"]) if not pd.isna(row["year"]) else None,
                athlete_world_ranking=int(row["athlete_world_ranking"]) if not pd.isna(
                    row["athlete_world_ranking"]) else None,
                opponent_world_ranking=int(row["opponent_world_ranking"]) if not pd.isna(
                    row["opponent_world_ranking"]) else None,
                notable_win=row["notable_win"] if not pd.isna(row["notable_win"]) else None,
                lost_to=row.get("lost_to")
            )
            for _, row in notable_wins_singles.iterrows()
        ]

        # Step 5: Insert records
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into z_badminton_singles_notable_wins_final.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()

if __name__ == '__main__':
    main()