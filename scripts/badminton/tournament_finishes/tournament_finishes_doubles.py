import mysql.connector
import pandas as pd


#connect to mysql server
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)


#query for only doubles
doubles_query = """
SELECT DISTINCT 
  a.tournament_id AS tournament_id,
  b.name AS tournament_name,
  b.grade AS tournament_grade,
  b.date AS tournament_date,
  b.year AS tournament_year,
  a.athlete_id AS athlete_id,
  a.name AS category,
  a.position AS final_position
FROM sai_badminton_viz_final.badminton_athlete_tournament_draw a
INNER JOIN sai_badminton_viz_final.badminton_athlete_tournament b
  ON a.tournament_id = b.tournament_id AND a.athlete_id = b.athlete_id
WHERE a.athlete_id IN (
  70500, 72435, 59966, 71612, 98187, 69560, 57372
);
"""

df = pd.read_sql(doubles_query, conn)
conn.close()

grade_levels = ["100", "300", "500", "750", "1000", "G1_CC"]
position_stages = ["R32", "R16", "QF", "SF", "F"]


# Fill missing grade values with "G1_CC" and all grades to strings
df = df.rename(columns={"tournament_grade": "grade", "final_position": "position"})
df["grade"] = df["grade"].fillna("G1_CC").astype(str) 
df = df[df["tournament_year"].isin([2024, 2025])]


# Filter data for only the years 2024 and 2025
for year in [2024, 2025]:
    summaries = []
    for aid in df[df["tournament_year"] == year]["athlete_id"].unique():
        sub = df[(df["athlete_id"] == aid) & (df["tournament_year"] == year)]
        pivot = sub.pivot_table(index="grade", columns="position", aggfunc='size', fill_value=0)
        pivot = pivot.reindex(index=grade_levels, columns=position_stages).fillna(0).astype(int)
        summary = {"athlete_id": aid}
        for pos in position_stages:
            for grade in grade_levels:
                col = f"{pos}_Super_{grade}"
                summary[col] = pivot.at[grade, pos] if pivot.at[grade, pos] != 0 else ""
        summaries.append(summary)
    output_file = f"badminton_D_athlete_tournament_finish_{year}.csv"
    pd.DataFrame(summaries).to_csv(output_file, index=False)
    print(f"Saved → {output_file}")


from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker


# Database Setup for ORM

DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()


# Shared ORM Base Class

class TournamentFinishBase(Base):
    __abstract__ = True
    athlete_id = Column(Integer, primary_key=True)

    # Dynamically add all 30+ possible columns
    for pos in ["R32", "R16", "QF", "SF", "F"]:
        for grade in ["100", "300", "500", "750", "1000", "G1_CC"]:
            col_name = f"{pos}_Super_{grade}"
            vars()[col_name] = Column(String(10))  # Store counts or empty as string


# Dynamically Define Per-Year Models

class TournamentFinish2024(TournamentFinishBase):
    __tablename__ = "badminton_doubles_tournament_finishes_2024"

class TournamentFinish2025(TournamentFinishBase):
    __tablename__ = "badminton_doubles_tournament_finishes_2025"

# Create tables in DB
Base.metadata.create_all(engine)


# Save to DB Function

def save_to_db(year:int, model_cls):
    """
    Save athlete tournament summary CSV into the corresponding MySQL table.
    """
    df = pd.read_csv(f"badminton_D_athlete_tournament_finish_{year}.csv").fillna("")

    records = [
        model_cls(**row._asdict()) if hasattr(row, '_asdict') else model_cls(**row)
        for _, row in df.iterrows()
    ]
    session.add_all(records)
    session.commit()
    print(f"Inserted {len(records)} rows into `{model_cls.__tablename__}`")


# Insert both years

save_to_db(2024, TournamentFinish2024)
save_to_db(2025, TournamentFinish2025)

session.close()
print(" All tournament finish summaries inserted into MySQL.")
