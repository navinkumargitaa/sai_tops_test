from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import mysql.connector

#  1. ORM Base & DB Setup 
Base = declarative_base()

class DoublesRankingGraph(Base):
    __tablename__ = 'badminton_doubles_ranking_graph_all_years'
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(String(20), nullable=False)
    tournament_date = Column(Date)
    world_ranking = Column(Integer)
    world_tour_ranking = Column(Integer)
    olympic_rank = Column(Integer)

class DoublesRankingGraph2025(Base):
    __tablename__ = 'badminton_doubles_ranking_graph_2025'
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(String(20), nullable=False)
    tournament_date = Column(Date)
    world_ranking = Column(Integer)
    world_tour_ranking = Column(Integer)
    olympic_rank = Column(Integer)

#  2. Connect to MySQL 
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

#  3. Create tables 
Base.metadata.create_all(engine)

#  Connect to MySQL 
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

#  Doubles Ranking Query 
query_doubles = """
SELECT DISTINCT 
  c.athlete_1_id as athlete_id,
  a.date AS tournament_date,
  a.world_ranking,
  a.world_tour_ranking,
  a.olympics AS olympic_rank
FROM sai_badminton_viz_final.badminton_ranking_graph_team a
JOIN sai_badminton_viz_final.badminton_team_ranking b 
  ON a.team_id = b.team_id
JOIN sai_badminton_viz_final.badminton_doubles c 
  ON b.points = c.points
WHERE a.team_id IN (1, 2, 61, 116);
"""

df = pd.read_sql(query_doubles, conn)
conn.close()

df['world_tour_ranking'] = df['world_tour_ranking'].ffill()


#  Convert Data Types 
df['athlete_id'] = df['athlete_id'].astype(int)
df['tournament_date'] = pd.to_datetime(df['tournament_date'], errors='coerce')

# Convert to nullable integer type (so missing = empty cell in CSV)
for col in ['world_ranking', 'world_tour_ranking', 'olympic_rank']:
    df[col] = pd.to_numeric(df[col], errors='coerce').round(0).astype('Int64')

#  Save Full Doubles Data 
output_all = "final_data_ranking_graph_doubles.csv"
df.to_csv(output_all, index=False, na_rep="")

#  Filter and Save 2025 Data 
df_2025 = df[df['tournament_date'].dt.year == 2025]
output_2025 = "final_data_ranking_graph_doubles_2025.csv"
df_2025.to_csv(output_2025, index=False, na_rep="")

#  Summary 
print(f"Duplicates in 2025 data: {df_2025.duplicated().sum()}")
print(f"Saved full doubles data to → {output_all}")
print(f"Saved 2025-only doubles data to → {output_2025}")


#  6. Insert full data 
records_all = []
for _, row in df.iterrows():
    records_all.append(DoublesRankingGraph(
        athlete_id=row['athlete_id'],
        tournament_date=row['tournament_date'],
        world_ranking=None if pd.isna(row['world_ranking']) else int(row['world_ranking']),
        world_tour_ranking=None if pd.isna(row['world_tour_ranking']) else int(row['world_tour_ranking']),
        olympic_rank=None if pd.isna(row['olympic_rank']) else int(row['olympic_rank'])
    ))

#  7. Insert 2025 data 
records_2025 = []
for _, row in df_2025.iterrows():
    records_2025.append(DoublesRankingGraph2025(
        athlete_id=row['athlete_id'],
        tournament_date=row['tournament_date'],
        world_ranking=None if pd.isna(row['world_ranking']) else int(row['world_ranking']),
        world_tour_ranking=None if pd.isna(row['world_tour_ranking']) else int(row['world_tour_ranking']),
        olympic_rank=None if pd.isna(row['olympic_rank']) else int(row['olympic_rank'])
    ))

#  8. Commit to DB 
session.add_all(records_all)
session.add_all(records_2025)
session.commit()
print(f"Inserted {len(records_all)} full records and {len(records_2025)} 2025-only records.")
