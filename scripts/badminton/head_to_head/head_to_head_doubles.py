import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

# Athlete IDs to filter
athlete_ids = [
    72435,57372,69560,71612
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

# SQL Query for all doubles data
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

    team_1_athlete_2_id,
    team_1_athlete_2_name,
    team_1_athlete_2_profile_pic,
    team_1_athlete_2_nationality_name,
    team_1_athlete_2_national_flag_thumbnail,
    team_1_athlete_2_dob,
    team_1_athlete_2_age,
    team_1_athlete_2_height,

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

    team_2_athlete_2_id,
    team_2_athlete_2_name,
    team_2_athlete_2_profile_pic,
    team_2_athlete_2_nationality_name,
    team_2_athlete_2_national_flag_thumbnail,
    team_2_athlete_2_dob,
    team_2_athlete_2_age,
    team_2_athlete_2_height,

    team_2_current_rank,
    team_2_total_wins,
    team_2_total_points,
    team_2_career_wins,
    team_2_career_losses,
    team_2_current_year_wins,
    team_2_current_year_losses
FROM sai_badminton_viz_final.badminton_h2h_viz
WHERE event_code IN ('Men''s Doubles', 'Women''s Doubles', 'Mixed Doubles')
  AND team_1_athlete_1_nationality_name = 'India';
'''

# Fetch the data
h2h_df = pd.read_sql(query, conn)
conn.close()

# Filter using athlete_ids
h2h_df = h2h_df[h2h_df['team_1_athlete_1_id'].isin(athlete_ids)]

# Save to CSV
csv_path = "badminton_doubles_h2h.csv"
h2h_df.to_csv(csv_path, index=False)
print(f"Saved → {csv_path}")

# ORM Definition
Base = declarative_base()

class DoublesMatchStats(Base):
    __tablename__ = "badminton_h2h_doubles_data_viz"

    id = Column(Integer, primary_key=True, autoincrement=True)
    team_1_athlete_1_id = Column(String(20))
    team_1_athlete_1_name = Column(String(100))
    team_1_athlete_1_profile_pic = Column(String(255))
    team_1_athlete_1_nationality_name = Column(String(50))
    team_1_athlete_1_national_flag_thumbnail = Column(String(255))
    team_1_athlete_1_dob = Column(Date)
    team_1_athlete_1_age = Column(Integer)
    team_1_athlete_1_height = Column(Integer)

    team_1_athlete_2_id = Column(String(20))
    team_1_athlete_2_name = Column(String(100))
    team_1_athlete_2_profile_pic = Column(String(255))
    team_1_athlete_2_nationality_name = Column(String(50))
    team_1_athlete_2_national_flag_thumbnail = Column(String(255))
    team_1_athlete_2_dob = Column(Date)
    team_1_athlete_2_age = Column(Integer)
    team_1_athlete_2_height = Column(Integer)

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
    team_2_athlete_1_age = Column(Integer)
    team_2_athlete_1_height = Column(Integer)

    team_2_athlete_2_id = Column(String(20))
    team_2_athlete_2_name = Column(String(100))
    team_2_athlete_2_profile_pic = Column(String(255))
    team_2_athlete_2_nationality_name = Column(String(50))
    team_2_athlete_2_national_flag_thumbnail = Column(String(255))
    team_2_athlete_2_dob = Column(Date)
    team_2_athlete_2_age = Column(Integer)
    team_2_athlete_2_height = Column(Integer)

    team_2_current_rank = Column(Integer)
    team_2_total_wins = Column(Integer)
    team_2_total_points = Column(Integer)
    team_2_career_wins = Column(Integer)
    team_2_career_losses = Column(Integer)
    team_2_current_year_wins = Column(Integer)
    team_2_current_year_losses = Column(Integer)

# Database connection
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Create table
Base.metadata.create_all(engine)

# Read CSV
df = pd.read_csv("badminton_doubles_h2h.csv")

# Convert DOB columns to datetime
dob_cols = [col for col in df.columns if "dob" in col.lower()]
for col in dob_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Insert into database
df.to_sql(
    name=DoublesMatchStats.__tablename__,
    con=engine,
    if_exists="append",
    index=False
)

print(f"Inserted {len(df)} rows into '{DoublesMatchStats.__tablename__}' table.")