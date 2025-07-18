"""
main.py

Main entry point for loading end-of-October athlete ranking snapshots
into the 'archery_arrow_average_viz' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

# Importing required modules
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.archery import ArcheryArrowAverage, Base

# Import database engine
from model.archery import sai_db_engine

# Import transformation logic for arrow average computation
from services.archery.analysis import get_arrow_average


def main():
    """
    Orchestrates the ETL process:
    - Creates the required table if it doesn't exist
    - Extracts and transforms athlete arrow average data
    - Loads the transformed data into the database
    """

    # Step 1: Create the table if it does not exist
    Base.metadata.create_all(sai_db_engine)
    print("Table created successfully (or already exists).")

    # Step 2: Force table creation
    Base.metadata.create_all(bind=sai_db_engine, tables=[ArcheryArrowAverage.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform data using business logic from service layer
    arrow_avg_data = get_arrow_average()  # Returns a pandas DataFrame
    print("Data extracted and transformed:")
    print(arrow_avg_data.head())  # Display first few records for verification

    # Step 3: Set up a session for writing data to the database
    Session = sessionmaker(bind=sai_db_engine)  # Bind session to the DB engine
    session = Session()  # Create a new session instance

    # Step 4: Convert DataFrame rows to a list of ORM model instances
    records = [
        ArcheryArrowAverage(
            athlete_id=int(row['athlete_id']),
            athlete_name=row['athlete_name'],
            competition_id=int(row['competition_id']),
            comp_name=row['comp_name'],
            comp_date=row['comp_date'],
            comp_year=int(row['comp_year']) if pd.notnull(row['comp_year']) else None,
            type_arrow_avg=row['type_arrow_avg'],
            arrow_average=float(row['arrow_average']) if pd.notnull(row['arrow_average']) else None
        )
        for _, row in arrow_avg_data.iterrows()
    ]

    # Step 5: Add all records to the session
    session.add_all(records)

    # Step 6: Commit the session to persist changes in the database
    session.commit()

    # Step 7: Close the session
    session.close()

    # Step 8: Confirm how many records were loaded
    print(f"Loaded {len(records)} records into the database.")


# Run the ETL process only if this script is executed directly
if __name__ == "__main__":
    main()
