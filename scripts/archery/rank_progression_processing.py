"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_ranking_progression_viz' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

import pandas as pd
from sqlalchemy.orm import sessionmaker

from orm.archery import ArcheryRankingProgression, Base
from model.archery import sai_db_engine
from services.archery.analysis import get_end_of_september_ranking


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
    Base.metadata.create_all(bind=sai_db_engine, tables=[ArcheryRankingProgression.__table__])
    print("Table creation attempted.")


    # Step 2: Extract and transform ranking data
    end_of_october_ranking = get_end_of_september_ranking()
    print("✅ Data extracted and transformed:")
    print(end_of_october_ranking.head())

    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (uncomment if needed)
        # session.query(ArcheryRankingProgression).delete()
        # session.commit()

        # Step 4: Convert each row to an ORM object
        records = [
            ArcheryRankingProgression(
                athlete_id=int(row['athlete_id']),
                athlete_name=row['athlete_name'],
                year=int(row['year']),
                rank=int(row['current_rank']) if pd.notnull(row['current_rank']) else None,
                ranking_status=row['ranking_status'],
                rank_date_issued=row['rank_date_issued'].date() if pd.notnull(row['rank_date_issued']) else None
            )
            for _, row in end_of_october_ranking.iterrows()
        ]

        # Step 5: Insert records
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into the database.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()


if __name__ == "__main__":
    main()
