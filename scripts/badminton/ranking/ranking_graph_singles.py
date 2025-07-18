from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import mysql.connector

#   ORM Base & DB Setup 
Base = declarative_base()

class SinglesRankingGraph(Base):
    __tablename__ = 'badminton_singles_ranking_graph_viz'
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(String(20), nullable=False)
    athlete_name=Column(String(30))
    tournament_date = Column(Date)
    world_ranking = Column(Integer)
    world_tour_ranking = Column(Integer)
    olympic_rank = Column(Integer)

class SinglesRankingGraph2025(Base):
    __tablename__ = 'badminton_singles_ranking_graph_2025_viz'
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(String(20), nullable=False)
    athlete_name=Column(String(30))
    tournament_date = Column(Date)
    world_ranking = Column(Integer)
    world_tour_ranking = Column(Integer)
    olympic_rank = Column(Integer)

#   Connect to MySQL 
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

#   Create tables 
Base.metadata.create_all(engine)

#   Query & Load Data 
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

query = """
SELECT DISTINCT 
  a.athlete_id AS athlete_id,
  b.display_name as athlete_name,
  a.date AS tournament_date,
  a.world_ranking AS world_ranking,
  a.world_tour_ranking AS world_tour_ranking,
  a.olympics AS olympic_rank
FROM sai_badminton_viz_final.badminton_ranking_graph_ind a
join sai_badminton_viz_final.badminton_athlete b
on a.athlete_id = b.athlete_id
WHERE a.athlete_id IN (
  83950, 68870, 73173, 69093, 59687, 74481, 58664, 68322, 99042, 91807, 97168, 70595, 82572
) AND a.ranking_category != 'WOMEN_DOUBLES';
"""
df = pd.read_sql(query, conn)
conn.close()

df['world_tour_ranking'] = df['world_tour_ranking'].ffill()


#  Clean Data 
df['athlete_id'] = df['athlete_id'].astype(str)
df['tournament_date'] = pd.to_datetime(df['tournament_date'], errors='coerce')

for col in ['world_ranking', 'world_tour_ranking', 'olympic_rank']:
    df[col] = pd.to_numeric(df[col], errors='coerce').round(0).astype('Int64')

df.to_csv("singles_ranking_graph_combined.csv", index=False, na_rep="")
df_2025 = df[df['tournament_date'].dt.year == 2025]
df_2025.to_csv("singles_ranking_graph_combined_2025.csv", index=False, na_rep="")

print(f"Duplicates in 2025 data: {df_2025.duplicated().sum()}")
print("Saved full and 2025 CSVs.")

#   Insert full data 
records_all = []
for _, row in df.iterrows():
    records_all.append(SinglesRankingGraph(
        athlete_id=row['athlete_id'],
        athlete_name=row['athlete_name'],
        tournament_date=row['tournament_date'],
        world_ranking=None if pd.isna(row['world_ranking']) else int(row['world_ranking']),
        world_tour_ranking=None if pd.isna(row['world_tour_ranking']) else int(row['world_tour_ranking']),
        olympic_rank=None if pd.isna(row['olympic_rank']) else int(row['olympic_rank'])
    ))

#  Insert 2025 data 
records_2025 = []
for _, row in df_2025.iterrows():
    records_2025.append(SinglesRankingGraph2025(
        athlete_id=row['athlete_id'],
        athlete_name=row['athlete_name'],
        tournament_date=row['tournament_date'],
        world_ranking=None if pd.isna(row['world_ranking']) else int(row['world_ranking']),
        world_tour_ranking=None if pd.isna(row['world_tour_ranking']) else int(row['world_tour_ranking']),
        olympic_rank=None if pd.isna(row['olympic_rank']) else int(row['olympic_rank'])
    ))

#   Commit to DB 
session.add_all(records_all)
session.add_all(records_2025)
session.commit()
print(f"Inserted {len(records_all)} full records and {len(records_2025)} 2025-only records.")