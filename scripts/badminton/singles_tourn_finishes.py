


# scripts/run_analysis_and_update.py

from model.badminton.database import read_singles_tournament_finishes, sai_db_engine
from services.badminton.analysis import generate_tournament_summary, save_summary_to_db
from orm.badminton import Base

def main():
    # Step 1: Read data
    df = read_singles_tournament_finishes()

    summaries = generate_tournament_summary(df)
    # Step 2: Generate summaries

    # Step 3: Save as CSVs
    for year, df_year in summaries.items():
        filename = f"badminton_S_athlete_tournament_finish_{year}.csv"
        df_year.to_csv(filename, index=False)
        print(f"Saved → {filename}")

    # Step 4: Create tables if not present
    Base.metadata.create_all(sai_db_engine)

    # Step 5: Save to database
    save_summary_to_db(summaries)

    print("All summaries inserted into MySQL.")

if __name__ == "__main__":
    main()
