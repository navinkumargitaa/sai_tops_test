"""
main.py

Main entry point for loading athlete events
into the 'shooting_athlete_event' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

# Importing required modules
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.shooting.athlete_unique_events import ShootingAthleteEvents, Base

# Import database engine
from model.shooting.database import sai_db_engine

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
    Base.metadata.create_all(bind=sai_db_engine, tables=[ShootingAthleteEvents.__table__])
    print("Table creation attempted.")

    # Step 3: Set up a session for writing data to the database
    Session = sessionmaker(bind=sai_db_engine)  # Bind session to the DB engine
    session = Session()  # Create a new session instance

    # Step 2: Load data from CSV
    df = pd.read_csv("/home/navin/Desktop/SAI/sai_tops_testing/data/shooting/athlete_unique_event_name.csv")
    print(f"Loaded {len(df)} records from CSV.")

    # Convert to ORM instances
    # Convert to ORM instances
    records = [
        ShootingAthleteEvents(
            athlete_name=row.get("athlete_name"),
            athlete_event=row.get("athlete_event")
        )
        for _, row in df.iterrows()
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