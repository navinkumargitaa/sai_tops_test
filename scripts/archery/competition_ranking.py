"""
Main ETL script for loading archery competition ranking snapshot
into the 'archery_competition_ranking_viz' table.

Author: navin@gitaa.in
"""

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect

# Import ORM model and Base
from orm.archery import ArcheryCompetitionRanking, Base

# Import DB engine
from model.archery import sai_db_engine

# Your ETL data loader
from services.archery.analysis import get_competition_ranking


def main():
    # Step 1: Debug what tables SQLAlchemy knows
    print("Known tables before create_all():", Base.metadata.tables.keys())

    # Step 2: Force table creation
    Base.metadata.create_all(bind=sai_db_engine, tables=[ArcheryCompetitionRanking.__table__])
    print("Table creation attempted.")

    # Step 3: Confirm creation via inspector
    inspector = inspect(sai_db_engine)
    print("Tables in DB:", inspector.get_table_names())

    # Step 4: Load transformed data
    comp_rank = get_competition_ranking()  # returns a DataFrame
    print("Sample data:\n", comp_rank.head())

    # Step 5: Start session
    Session = sessionmaker(bind=sai_db_engine)
    session = Session()

    # Step 6: Convert DataFrame to ORM objects
    records = []
    for _, row in comp_rank.iterrows():
        try:
            rec = ArcheryCompetitionRanking(
                athlete_id=int(row['athlete_id']),
                athlete_name=row['athlete_name'],
                comp_id=int(row['comp_id']),
                comp_full_name=row['comp_full_name'],
                comp_short_name=row['comp_short_name'],
                comp_place=row['comp_place'],
                comp_date=row['comp_date'].date() if pd.notnull(row['comp_date']) else None,
                comp_rank=int(row['comp_rank']) if pd.notnull(row['comp_rank']) else None
            )
            records.append(rec)
        except Exception as e:
            print(f"Record conversion failed for:\n{row}\nError: {e}")

    # Step 7: Insert data
    try:
        session.add_all(records)
        session.commit()
        print(f"Inserted {len(records)} records.")
    except Exception as e:
        session.rollback()
        print("Commit failed:", e)
    finally:
        session.close()
        print("Session closed.")


if __name__ == "__main__":
    main()
