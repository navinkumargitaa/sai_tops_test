"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_ranking_october' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"


import pandas as pd
from orm.archery import ArcheryCompetitionRanking, Base
from model.archery import sai_db_engine
from services.archery.ranking_processing import get_competition_ranking
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
    print("Table created successfully (or already exists).")


    comp_rank = get_competition_ranking()
    print("Data extracted and transformed:")
    print(comp_rank.head())

    # 3. Load into database
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    records = [
        ArcheryCompetitionRanking(
            athlete_id=int(row['athlete_id']),
            comp_id=int(row['comp_id']),
            comp_full_name=row['comp_full_name'],
            comp_short_name=row['comp_short_name'],
            comp_new_short_name=row['comp_new_short_name'],
            comp_place=row['comp_place'],
            comp_date=row['comp_date'].date() if pd.notnull(row['comp_date']) else None,
            comp_rank=int(row['comp_rank']) if pd.notnull(row['comp_rank']) else None
        )
        for _, row in comp_rank.iterrows()
    ]

    session.add_all(records)
    session.commit()
    session.close()

    print(f"Loaded {len(records)} records into the database.")

if __name__ == "__main__":
    main()
