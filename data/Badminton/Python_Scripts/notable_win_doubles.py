import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.schema import PrimaryKeyConstraint
import re

# Connect to the MySQL database
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

"""
query for getting the latest rank of the players
query = SELECT 
  a.athlete_1_id,
  a.athlete_2_id,
  b.team_id,
  b.ranking_category,
  b.date,
  b.world_ranking
FROM sai_badminton_viz_final.badminton_doubles a
JOIN sai_badminton_viz_final.badminton_ranking_graph_team b
  ON a.row_id = b.team_id
WHERE b.date = (
  SELECT MAX(date)
  FROM sai_badminton_viz_final.badminton_ranking_graph_team
);
"""


# Load all doubles ranking data from the ranking table
doubles_ranking_map_query = "SELECT * FROM sai_badminton_viz_final.doubles_rankings;"
doubles_ranking_map_df = pd.read_sql(doubles_ranking_map_query, mysql_connection)

# Load the match data by joining athlete, team, and tournament tables
master_query = """
SELECT DISTINCT 
    a.row_id AS team_id,
    c.athlete_id,
    c.tournament_id,
    c.round_name AS final_position,
    c.winner,
    c.team_1_player_1_id,
    c.team_1_player_2_id,
    c.team_2_player_1_id,
    c.team_2_player_2_id,
    c.team_1_player_1_name,
    c.team_1_player_2_name,
    c.team_2_player_1_name,
    c.team_2_player_2_name,
    c.draw_name_full AS category,
    d.name AS tournament_name,
    d.date AS tournament_date,
    d.grade AS tournament_grade,
    d.year AS tournament_year
FROM sai_badminton_viz_final.badminton_doubles AS a
JOIN sai_badminton_viz_final.badminton_athlete_match AS c
    ON a.athlete_1_id = c.athlete_id
JOIN sai_badminton_viz_final.badminton_athlete_tournament AS d
    ON c.tournament_id = d.tournament_id;
"""

master_df = pd.read_sql(master_query, mysql_connection)
mysql_connection.close()

# Filter only the selected team_ids for processing
team_ids_to_process = [1, 2, 61, 116]
all_matches_df = master_df[master_df["team_id"].isin(team_ids_to_process)].copy()

def fetch_team_rank_from_dataframe(player1_id: int, player2_id: int):
    row = doubles_ranking_map_df[
        ((doubles_ranking_map_df['player1_id'] == player1_id) & (doubles_ranking_map_df['player2_id'] == player2_id)) |
        ((doubles_ranking_map_df['player1_id'] == player2_id) & (doubles_ranking_map_df['player2_id'] == player1_id))
    ]
    return row['rank'].iloc[0] if not row.empty else None

# Parse tournament dates
parsed_dates = []
for i in range(len(all_matches_df)):
    date_str = all_matches_df.iloc[i]['tournament_date']
    year = all_matches_df.iloc[i]['tournament_year']
    match = re.search(r'-\s*(\d{1,2})\s+([A-Za-z]+)', str(date_str))
    if match:
        day = match.group(1)
        month = match.group(2)
        full_date = f"{day} {month} {year}"
        parsed_date = pd.to_datetime(full_date, format="%d %B %Y", errors='coerce')
    else:
        parsed_date = pd.to_datetime(date_str, errors='coerce')
    parsed_dates.append(parsed_date)

all_matches_df['parsed_date'] = parsed_dates

# Win/loss determination
def determine_win_loss_notable(row: pd.Series) -> pd.Series:
    athlete_id = row['athlete_id']
    winner_team = row['winner']

    t1p1, t1p2 = row['team_1_player_1_id'], row['team_1_player_2_id']
    t2p1, t2p2 = row['team_2_player_1_id'], row['team_2_player_2_id']

    team1_label = f"{row['team_1_player_1_name']} & {row['team_1_player_2_name']}"
    team2_label = f"{row['team_2_player_1_name']} & {row['team_2_player_2_name']}"

    team1_rank = fetch_team_rank_from_dataframe(t1p1, t1p2)
    team2_rank = fetch_team_rank_from_dataframe(t2p1, t2p2)

    if athlete_id in [t1p1, t1p2]:
        athlete_team = 1
        athlete_name, opponent_name = team1_label, team2_label
        athlete_rank, opponent_rank = team1_rank, team2_rank
    elif athlete_id in [t2p1, t2p2]:
        athlete_team = 2
        athlete_name, opponent_name = team2_label, team1_label
        athlete_rank, opponent_rank = team2_rank, team1_rank
    else:
        return pd.Series(["Unknown", "Unknown", "", ""])

    athlete_display = f"{athlete_name} ({athlete_rank})" if athlete_rank else athlete_name
    opponent_display = f"{opponent_name} ({opponent_rank})" if opponent_rank else opponent_name

    if winner_team == athlete_team:
        notable_win = opponent_display if (
            athlete_rank is not None and opponent_rank is not None and opponent_rank < athlete_rank
        ) else ""
        return pd.Series([athlete_display, opponent_display, notable_win, ""])
    else:
        return pd.Series([opponent_display, athlete_display, "", opponent_display])

# Apply win/loss logic
all_matches_df[['win', 'loss', 'notable_win', 'lost_to']] = all_matches_df.apply(determine_win_loss_notable, axis=1)
all_matches_df = all_matches_df[all_matches_df['winner'] != 0]
all_matches_df['tournament_grade'] = all_matches_df['tournament_grade'].fillna('G1_CC')

# Summary per team+tournament
def summarize_team_tournament(group: pd.DataFrame) -> pd.Series:
    round_priority_order = ['R32', 'R16', 'QF', 'SF', 'F']
    group = group.sort_values("parsed_date")  # Sort group by date
    group['round_rank'] = group['final_position'].apply(
        lambda round_name: round_priority_order.index(round_name.upper()) if str(round_name).upper() in round_priority_order else -1
    )
    if group['round_rank'].max() == -1:
        return None

    best_performing_row = group.loc[group['round_rank'].idxmax()]
    notable_wins_list = group['notable_win'].dropna().loc[group['notable_win'] != ""].tolist()
    lost_to_list = group['lost_to'].dropna().loc[group['lost_to'] != ""].tolist()

    return pd.Series({
        'team_id': best_performing_row['team_id'],
        'tournament_name': best_performing_row['tournament_name'],
        'final_position': best_performing_row['final_position'],
        'tournament_grade': best_performing_row['tournament_grade'],
        'tournament_year': best_performing_row['tournament_year'],
        'tournament_date': best_performing_row['parsed_date'],
        'notable_wins': ", ".join(notable_wins_list),
        'lost_to': ", ".join(lost_to_list)
    })

# Group and summarize
tournament_summary_df = (
    all_matches_df
    .sort_values("parsed_date")
    .groupby(['team_id', 'tournament_name'])
    .apply(summarize_team_tournament)
    .dropna()
    .reset_index(drop=True)
)

def place_team_id_first(df: pd.DataFrame) -> pd.DataFrame:
    columns = df.columns.tolist()
    if 'team_id' in columns:
        columns.remove('team_id')
        columns = ['team_id'] + columns
    return df[columns]

# Save CSVs and insert to DB
for selected_year in [2024, 2025]:
    year_specific_df = tournament_summary_df[
        (tournament_summary_df['tournament_year'] == selected_year) &
        (tournament_summary_df['lost_to'].str.strip() != "")
    ].copy()

    year_specific_df['team_id'] = year_specific_df['team_id'].astype(int)
    year_specific_df['tournament_year'] = year_specific_df['tournament_year'].astype(int)

    # Convert to datetime for sorting
    year_specific_df['tournament_date'] = pd.to_datetime(year_specific_df['tournament_date'], errors='coerce')

    # Sort by team_id ascending, tournament_date descending (latest first)
    year_specific_df = year_specific_df.sort_values(by=['team_id', 'tournament_date'], ascending=[True, False])

    # Convert date back to string for CSV
    year_specific_df['tournament_date'] = year_specific_df['tournament_date'].dt.strftime('%d-%b-%Y')

    year_specific_df = place_team_id_first(year_specific_df)
    year_specific_df.to_csv(f"doubles_notable_wins_{selected_year}.csv", index=False)
    print(f"Saved → doubles_notable_wins_{selected_year}.csv")


# ORM for DB insertion
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

class NotableDoublesBase(Base):
    __abstract__ = True
    team_id = Column(Integer)
    tournament_name = Column(String(255))
    final_position = Column(String(20))
    tournament_grade = Column(String(20))
    tournament_year = Column(Integer)
    tournament_date = Column(Date)  
    notable_wins = Column(String(1000))
    lost_to = Column(String(1000))

    __table_args__ = (
        PrimaryKeyConstraint('team_id', 'tournament_name'),
    )

class DoublesNotableWins2024(NotableDoublesBase):
    __tablename__ = "badminton_doubles_notable_wins_prv_year_viz"

class DoublesNotableWins2025(NotableDoublesBase):
    __tablename__ = "badminton_doubles_notable_wins_curr_year_viz"

Base.metadata.create_all(engine)

def save_notable_wins_to_db(year: int, model_cls):
    df = pd.read_csv(f"doubles_notable_wins_{year}.csv").fillna("")
    df['tournament_date'] = pd.to_datetime(df['tournament_date'], format='%d-%b-%Y', errors='coerce').dt.date
    records = [model_cls(**row.to_dict()) for _, row in df.iterrows()]
    session.add_all(records)
    session.commit()
    print(f"Inserted {len(records)} rows into `{model_cls.__tablename__}`")

save_notable_wins_to_db(2024, DoublesNotableWins2024)
save_notable_wins_to_db(2025, DoublesNotableWins2025)
session.close()
print("All doubles notable wins inserted.")
