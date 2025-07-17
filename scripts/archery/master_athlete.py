"""
main.py

Main entry point for loading master athlete bio
into the 'archery_arrow_average_viz' table.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

# Importing required modules
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Import ORM model and base class
from orm.archery import ArcheryAthleteFinalViz, Base

# Import database engine
from model.archery import sai_db_engine

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

    # Step 3: Set up a session for writing data to the database
    Session = sessionmaker(bind=sai_db_engine)  # Bind session to the DB engine
    session = Session()  # Create a new session instance

    # Step 2: Load data from CSV
    df = pd.read_csv("/home/navin/Desktop/SAI/sai_tops_testing/data/archery/master_athlete_bio.csv")
    print(f"Loaded {len(df)} records from CSV.")


    # Replace NaNs with None
    df = df.where(pd.notnull(df), None)

    # Step 3: Convert date columns
    date_columns = [
         'first_induction',
        'first_exclusion', 'second_inclusion'
    ]

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors='coerce')

    # Convert to ORM instances
    records = [
        ArcheryAthleteFinalViz(
            athlete_id=int(row["athlete_id"]),
            name=row.get("name"),
            gender=row.get("gender"),
            dob=pd.to_datetime(row["dob"]).date() if pd.notnull(row["dob"]) else None,
            age=int(row["age"]) if pd.notnull(row["age"]) else None,
            city=row.get("city"),
            domicile_state=row.get("domicile_state"),
            country=row.get("country"),
            profile_picture_url=row.get("profile_picture_url"),
            event=row.get("event"),
            current_group=row.get("current_group"),
            training_base=row.get("training_base"),
            supported_by_ngo=row.get("supported_by_ngo"),
            employer=row.get("employer"),
            support_till_date=row.get("support_till_date"),
            raw_induction_text=row.get("raw_induction_text"),
            transfer_to_core_date=row.get("transfer_to_core_date"),
            current_induction_date=row.get("current_induction_date"),
            current_moc_meeting_no=row.get("current_moc_meeting_no"),
            first_induction=row.get("first_induction"),
            first_exclusion=row.get("first_exclusion"),
            second_inclusion=row.get("second_inclusion"),
            coach=row.get("coach"),
            physio=row.get("physio"),
            strength_conditioning=row.get("strength_conditioning"),
            nutritionist=row.get("nutritionist"),
            psychologist=row.get("psychologist"),
            masseur=row.get("masseur"),
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