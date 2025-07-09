"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_ranking_october' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"


import pandas as pd
from orm.archery import ArcheryArrowAverage, Base
from model.archery import sai_db_engine
from services.archery.analysis import get_arrow_average
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


    arrow_avg_data = get_arrow_average()
    print("Data extracted and transformed:")
    print(arrow_avg_data.head())

    # 3. Load into database
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    records = [
        ArcheryArrowAverage(
            athlete_id=int(row['athlete_id']),
            athlete_name=row['athlete_name'],
            competition_id=int(row['competition_id']),
            comp_new_short_name=row['comp_new_short_name'],
            qual_avg_arrow=float(row['qual_avg_arrow']) if pd.notnull(row['qual_avg_arrow']) else None,
            elem_avg_arrow=float(row['elem_avg_arrow']) if pd.notnull(row['elem_avg_arrow']) else None,
            competition_avg_arrow=float(row['competition_avg_arrow']) if pd.notnull(
                row['competition_avg_arrow']) else None
        )
        for _, row in arrow_avg_data.iterrows()
    ]

    session.add_all(records)
    session.commit()
    session.close()

    print(f"Loaded {len(records)} records into the database.")

if __name__ == "__main__":
    main()
