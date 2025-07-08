"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_ranking_october' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"


import pandas as pd
from orm.archery import ArcheryRankingProgression, Base
from model.archery import sai_db_engine
from services.archery.ranking_processing import get_end_of_october_ranking
from sqlalchemy.orm import sessionmaker

def main():
    """
    Orchestrates the ETL process:
    - Creates the table if it doesn't exist
    - Extracts and transforms data
    - Loads data into the database
    """
    # 1. Ensure table exists
    Base.metadata.create_all(sai_db_engine)
    print("✅ Table created successfully (or already exists).")

    # 2. Extract and transform
    end_of_october_ranking = get_end_of_october_ranking()
    print("✅ Data extracted and transformed:")
    print(end_of_october_ranking.head())

    # 3. Load into database
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    records = [
        ArcheryRankingProgression(
            athlete_id=int(row['athlete_id']),
            year=int(row['year']),
            rank=int(row['rank']) if pd.notnull(row['rank']) else None,
            points=float(row['points']) if pd.notnull(row['points']) else None,
            rank_date_issued=row['rank_date_issued'].date() if pd.notnull(row['rank_date_issued']) else None
        )
        for _, row in end_of_october_ranking.iterrows()
    ]

    session.add_all(records)
    session.commit()
    session.close()

    print(f"✅ Loaded {len(records)} records into the database.")

if __name__ == "__main__":
    main()
