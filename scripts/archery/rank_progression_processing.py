"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_ranking_progression_viz' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

# Import libraries
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.archery import ArcheryRankingProgression, Base

# Import SQLAlchemy engine
from model.archery import sai_db_engine

# Import data transformation function
from services.archery.analysis import get_end_of_october_ranking


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

    # Step 2: Extract and transform ranking data
    end_of_october_ranking = get_end_of_october_ranking()  # Returns a pandas DataFrame
    print("Data extracted and transformed:")
    print(end_of_october_ranking.head())  # Preview first few rows

    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    # Step 4: Convert each DataFrame row to an ORM object
    records = [
        ArcheryRankingProgression(
            athlete_id=int(row['athlete_id']),  # Athlete ID
            year=int(row['year']),  # Year of the snapshot
            rank=int(row['rank']) if pd.notnull(row['rank']) else None,  # Rank (nullable)
            points=float(row['points']) if pd.notnull(row['points']) else None,  # Points (nullable)
            rank_date_issued=row['rank_date_issued'].date() if pd.notnull(row['rank_date_issued']) else None  # Date issued
        )
        for _, row in end_of_october_ranking.iterrows()  # Iterate through all rows
    ]

    # Step 5: Add and commit all records to the database
    session.add_all(records)
    session.commit()

    # Step 6: Close the session
    session.close()

    # Step 7: Log success message
    print(f"Loaded {len(records)} records into the database.")


# Run only if this script is executed directly
if __name__ == "__main__":
    main()
