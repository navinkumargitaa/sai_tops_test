"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_competition_ranking_viz' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

# Import required libraries
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import the ORM model and base
from orm.archery import ArcheryCompetitionRanking, Base

# Import database engine
from model.archery import sai_db_engine

# Import data extraction and transformation function
from services.archery.analysis import get_competition_ranking


def main():
    """
    Orchestrates the ETL process:
    - Creates the target table if it doesn't exist
    - Extracts and transforms data for competition rankings
    - Loads the processed data into the database
    """

    # Step 1: Create the table (if it doesn't exist already)
    Base.metadata.create_all(sai_db_engine)
    print("Table created successfully (or already exists).")

    # Step 2: Extract and transform data using business logic
    comp_rank = get_competition_ranking()  # Returns a pandas DataFrame
    print("Data extracted and transformed:")
    print(comp_rank.head())  # Print first few rows for validation

    # Step 3: Initialize a SQLAlchemy session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    # Step 4: Convert DataFrame rows to ORM model instances
    records = [
        ArcheryCompetitionRanking(
            athlete_id=int(row['athlete_id']),  # Athlete ID
            comp_id=int(row['comp_id']),  # Competition ID
            comp_full_name=row['comp_full_name'],  # Full name of the competition
            comp_short_name=row['comp_short_name'],  # Short name of the competition
            comp_new_short_name=row['comp_new_short_name'],  # Cleaned short name for viz
            comp_place=row['comp_place'],  # Location of the competition
            comp_date=row['comp_date'].date() if pd.notnull(row['comp_date']) else None,  # Ranking date
            comp_rank=int(row['comp_rank']) if pd.notnull(row['comp_rank']) else None  # Athlete's rank
        )
        for _, row in comp_rank.iterrows()  # Iterate through all DataFrame rows
    ]

    # Step 5: Add all records to the database session
    session.add_all(records)

    # Step 6: Commit the transaction to write data to the table
    session.commit()

    # Step 7: Close the session
    session.close()

    # Step 8: Print confirmation
    print(f"Loaded {len(records)} records into the database.")


# Only run main if the script is executed directly
if __name__ == "__main__":
    main()
