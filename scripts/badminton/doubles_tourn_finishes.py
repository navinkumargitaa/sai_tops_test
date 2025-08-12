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
from orm.badminton.doubles_tournament_finishes import BadmintonDoublesTournamentFinishes,Base

# Import database engine
from model.badminton import sai_db_engine


from services.badminton.analysis import process_doubles_tournament_finishes

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
    Base.metadata.create_all(bind=sai_db_engine, tables=[BadmintonDoublesTournamentFinishes.__table__])
    print("Table creation attempted.")

    # Step 2: Extract and transform ranking data
    doubles_tournament_finishes = process_doubles_tournament_finishes()
    print("✅ Data extracted and transformed:")

    # Step 3: Initialize the database session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    try:
        # Optional: Clear existing records (only if you want a fresh load)
        # session.query(DoublesTournamentFinishes).delete()
        # session.commit()

        records = [
            BadmintonDoublesTournamentFinishes(
                athlete_id=int(row["athlete_id"]),
                athlete_name=row["athlete_name"],
                tournament_year=int(row["tournament_year"]),
                Junior_LT_R32=int(row["Junior_<R32"]),
                Junior_R32=int(row["Junior_R32"]),
                Junior_R16=int(row["Junior_R16"]),
                Junior_QF=int(row["Junior_QF"]),
                Junior_SF=int(row["Junior_SF"]),
                Junior_F=int(row["Junior_F"]),
                G3_LT_R32=int(row["G3_<R32"]),
                G3_R32=int(row["G3_R32"]),
                G3_R16=int(row["G3_R16"]),
                G3_QF=int(row["G3_QF"]),
                G3_SF=int(row["G3_SF"]),
                G3_F=int(row["G3_F"]),
                g100_LT_R32=int(row["100_<R32"]),
                g100_R32=int(row["100_R32"]),
                g100_R16=int(row["100_R16"]),
                g100_QF=int(row["100_QF"]),
                g100_SF=int(row["100_SF"]),
                g100_F=int(row["100_F"]),
                g300_LT_R32=int(row["300_<R32"]),
                g300_R32=int(row["300_R32"]),
                g300_R16=int(row["300_R16"]),
                g300_QF=int(row["300_QF"]),
                g300_SF=int(row["300_SF"]),
                g300_F=int(row["300_F"]),
                g500_LT_R32=int(row["500_<R32"]),
                g500_R32=int(row["500_R32"]),
                g500_R16=int(row["500_R16"]),
                g500_QF=int(row["500_QF"]),
                g500_SF=int(row["500_SF"]),
                g500_F=int(row["500_F"]),
                g750_LT_R32=int(row["750_<R32"]),
                g750_R32=int(row["750_R32"]),
                g750_R16=int(row["750_R16"]),
                g750_QF=int(row["750_QF"]),
                g750_SF=int(row["750_SF"]),
                g750_F=int(row["750_F"]),
                g1000_LT_R32=int(row["1000_<R32"]),
                g1000_R32=int(row["1000_R32"]),
                g1000_R16=int(row["1000_R16"]),
                g1000_QF=int(row["1000_QF"]),
                g1000_SF=int(row["1000_SF"]),
                g1000_F=int(row["1000_F"]),
                grade1_LT_R32=int(row["Grade 1_<R32"]),
                grade1_R32=int(row["Grade 1_R32"]),
                grade1_R16=int(row["Grade 1_R16"]),
                grade1_QF=int(row["Grade 1_QF"]),
                grade1_SF=int(row["Grade 1_SF"]),
                grade1_F=int(row["Grade 1_F"])
            )
            for _, row in doubles_tournament_finishes.iterrows()
        ]

        # Insert into DB
        session.add_all(records)
        session.commit()
        print(f"✅ Loaded {len(records)} records into the doubles_tournament_finishes table.")

    except Exception as e:
        session.rollback()
        print("❌ Commit failed:")
        print(e)

    finally:
        session.close()


if __name__ == "__main__":
    main()
