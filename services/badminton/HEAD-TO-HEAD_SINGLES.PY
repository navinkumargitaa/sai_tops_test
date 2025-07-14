import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from datetime import datetime

# Athlete IDs to filter after SQL query
athlete_ids = [
    83950, 68870, 73173, 69093, 59687, 74481, 58664,
    68322, 99042, 91807, 97168, 70595, 82572
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

# SQL Query: get all Indian singles matches
query = '''
SELECT DISTINCT
    team_1_athlete_1_id,
    team_1_athlete_1_name,
    team_1_athlete_1_profile_pic,
    team_1_athlete_1_nationality_name,
    team_1_athlete_1_national_flag_thumbnail,
    team_1_athlete_1_dob,
    team_1_athlete_1_age,
    team_1_athlete_1_height,

    team_1_current_rank,
    team_1_total_wins,
    team_1_total_points,
    team_1_career_wins,
    team_1_career_losses,
    team_1_current_year_wins,
    team_1_current_year_losses,

    team_2_athlete_1_id,
    team_2_athlete_1_name,
    team_2_athlete_1_profile_pic,
    team_2_athlete_1_nationality_name,
    team_2_athlete_1_national_flag_thumbnail,
    team_2_athlete_1_dob,
    team_2_athlete_1_age,
    team_2_athlete_1_height,

    team_2_current_rank,
    team_2_total_wins,
    team_2_total_points,
    team_2_career_wins,
    team_2_career_losses,
    team_2_current_year_wins,
    team_2_current_year_losses
FROM sai_badminton_viz_final.badminton_h2h_viz
WHERE event_code IN ('Men''s Singles', 'Women''s Singles')
  AND team_1_athlete_1_nationality_name = 'India';
'''

# Fetch and filter the data
full_df = pd.read_sql(query, conn)
conn.close()

h2h_df = full_df[full_df['team_1_athlete_1_id'].astype(int).isin(athlete_ids)]

# Save to CSV
csv_path = "badminton_singles_h2h.csv"
h2h_df.to_csv(csv_path, index=False)
print(f"Saved → {csv_path}")

# ORM Model Definition
Base = declarative_base()

class SinglesMatchStats(Base):
    __tablename__ = "badminton_h2h_singles_data_viz"

    id = Column(Integer, primary_key=True, autoincrement=True)

    team_1_athlete_1_id = Column(String(20))
    team_1_athlete_1_name = Column(String(100))
    team_1_athlete_1_profile_pic = Column(String(255))
    team_1_athlete_1_nationality_name = Column(String(50))
    team_1_athlete_1_national_flag_thumbnail = Column(String(255))
    team_1_athlete_1_dob = Column(Date)
    team_1_athlete_1_age = Column(Integer)
    team_1_athlete_1_height = Column(Integer)

    team_1_current_rank = Column(Integer)
    team_1_total_wins = Column(Integer)
    team_1_total_points = Column(Integer)
    team_1_career_wins = Column(Integer)
    team_1_career_losses = Column(Integer)
    team_1_current_year_wins = Column(Integer)
    team_1_current_year_losses = Column(Integer)

    team_2_athlete_1_id = Column(String(20))
    team_2_athlete_1_name = Column(String(100))
    team_2_athlete_1_profile_pic = Column(String(255))
    team_2_athlete_1_nationality_name = Column(String(50))
    team_2_athlete_1_national_flag_thumbnail = Column(String(255))
    team_2_athlete_1_dob = Column(Date)
    team_2_athlete_1_height = Column(Integer)
    team_2_athlete_1_age = Column(Integer)

    team_2_current_rank = Column(Integer)
    team_2_total_wins = Column(Integer)
    team_2_total_points = Column(Integer)
    team_2_career_wins = Column(Integer)
    team_2_career_losses = Column(Integer)
    team_2_current_year_wins = Column(Integer)
    team_2_current_year_losses = Column(Integer)

# SQLAlchemy setup
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)

# Read CSV & format date
df = pd.read_csv(csv_path)
for col in df.columns:
    if "dob" in col.lower():
        df[col] = pd.to_datetime(df[col], errors='coerce')

# Upload using to_sql
rows_inserted = df.to_sql(
    name="badminton_singles_match_stats",
    con=engine,
    if_exists="append",
    index=False
)

print(f"Inserted {rows_inserted} rows into 'badminton_singles_match_stats' table using to_sql().")