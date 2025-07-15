import pandas as pd
import re
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Connect to MySQL
DATABASE_URL = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# ORM Classes
class AthleteTournament(Base):
    __tablename__ = "badminton_athlete_tournament"
    tournament_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    grade = Column(String(50))
    date = Column(String(50))  # Dates like '03 - 08 June'
    year = Column(Integer)

class AthleteMatch(Base):
    __tablename__ = "badminton_athlete_match"
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(Integer)
    tournament_id = Column(Integer, ForeignKey('badminton_athlete_tournament.tournament_id'))
    round_name = Column(String(50))
    winner = Column(Integer)
    team_1_player_1_id = Column(Integer)
    team_1_player_1_name = Column(String(255))
    team_2_player_1_id = Column(Integer)
    team_2_player_1_name = Column(String(255))
    draw_name_full = Column(String(100))
    tournament = relationship("AthleteTournament")

class NotableWinsBase(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(Integer)
    tournament_name = Column(String(255))
    final_position = Column(String(50))
    tournament_grade = Column(String(50))
    tournament_year = Column(Integer)
    tournament_date = Column(Date)
    notable_wins = Column(String(1000))
    lost_to = Column(String(1000))

class NotableWins2024(NotableWinsBase):
    __tablename__ = "badminton_prv_yr_notable_wins_singles"

class NotableWins2025(NotableWinsBase):
    __tablename__ = "badminton_curr_yr_notable_wins_singles"

Base.metadata.create_all(engine)

"""
sql query for getting the latest rank of the athlete

query = SELECT athlete_id, ranking_category, date, world_ranking
FROM sai_badminton_viz_final.badminton_ranking_graph_ind
WHERE ranking_category IN ('MENS_SINGLES', 'WOMEN_SINGLES')
  AND date = (
    SELECT MAX(date)
    FROM sai_badminton_viz_final.badminton_ranking_graph_ind
    WHERE ranking_category IN ('MENS_SINGLES', 'WOMEN_SINGLES')
  )
ORDER BY date DESC;
"""


# Load Rankings
def load_athlete_rank_map():
    df = pd.read_sql("SELECT player_id, `rank` FROM badminton_singles_rankings", con=engine)
    df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce')
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
    return dict(zip(df['player_id'], df['rank']))

athlete_rank_map = load_athlete_rank_map()
athlete_ids = [83950, 68870, 73173, 69093, 59687, 74481, 58664, 68322, 99042, 91807, 97168, 70595, 82572]
round_priority_order = ['R32', 'R16', 'QF', 'SF', 'F']

# Fetch match data
results = (
    session.query(
        AthleteMatch.athlete_id,
        AthleteMatch.tournament_id,
        AthleteTournament.name.label("tournament_name"),
        AthleteMatch.round_name.label("final_position"),
        AthleteTournament.grade.label("tournament_grade"),
        AthleteTournament.date.label("tournament_date"),
        AthleteTournament.year.label("tournament_year"),
        AthleteMatch.winner,
        AthleteMatch.team_1_player_1_id,
        AthleteMatch.team_1_player_1_name,
        AthleteMatch.team_2_player_1_id,
        AthleteMatch.team_2_player_1_name,
        AthleteMatch.draw_name_full.label("category")
    )
    .join(AthleteTournament, AthleteMatch.tournament_id == AthleteTournament.tournament_id)
    .distinct()
    .all()
)

df = pd.DataFrame(results, columns=[
    "athlete_id", "tournament_id", "tournament_name", "final_position",
    "tournament_grade", "tournament_date", "tournament_year", "winner",
    "team_1_player_1_id", "team_1_player_1_name",
    "team_2_player_1_id", "team_2_player_1_name", "category"
])

# Fix tournament date to real end date using regex
parsed_dates = []
for i in range(len(df)):
    date_str = df.loc[i, 'tournament_date']
    year = df.loc[i, 'tournament_year']
    match = re.search(r'-\s*(\d{1,2})\s+([A-Za-z]+)', str(date_str))
    if match:
        day = match.group(1)
        month = match.group(2)
        full_date = f"{day} {month} {year}"
        parsed_date = pd.to_datetime(full_date, format="%d %B %Y", errors='coerce')
    else:
        parsed_date = pd.to_datetime(date_str, errors='coerce')
    parsed_dates.append(parsed_date)

df['parsed_date'] = parsed_dates

# Determine win/loss
def determine_win_loss(row):
    aid = row['athlete_id']
    winner = row['winner']
    t1, t2 = row['team_1_player_1_id'], row['team_2_player_1_id']
    n1, n2 = row['team_1_player_1_name'], row['team_2_player_1_name']
    r1, r2 = athlete_rank_map.get(t1), athlete_rank_map.get(t2)

    if aid == t1:
        athlete_side, athlete_name, opponent_name = 1, n1, n2
        athlete_rank, opponent_rank = r1, r2
    elif aid == t2:
        athlete_side, athlete_name, opponent_name = 2, n2, n1
        athlete_rank, opponent_rank = r2, r1
    else:
        return pd.Series(["Unknown", "Unknown", "", ""])

    athlete_display = f"{athlete_name} ({athlete_rank})" if athlete_rank else athlete_name
    opponent_display = f"{opponent_name} ({opponent_rank})" if opponent_rank else opponent_name

    if winner == athlete_side:
        notable = opponent_display if opponent_rank and athlete_rank and opponent_rank < athlete_rank else ""
        return pd.Series([athlete_display, opponent_display, notable, ""])
    else:
        return pd.Series([opponent_display, athlete_display, "", opponent_display])

df[['win', 'loss', 'notable_win', 'lost_to']] = df.apply(determine_win_loss, axis=1)
df = df[df['winner'] != 0]
df['tournament_grade'] = df['tournament_grade'].fillna("G1_CC")

filtered_df = df[df['athlete_id'].isin(athlete_ids)]
filtered_df = filtered_df[filtered_df['final_position'].isin(round_priority_order)]

# Summarize per athlete & tournament
def summarize(group):
    group['round_rank'] = group['final_position'].apply(
        lambda r: round_priority_order.index(r.upper()) if r.upper() in round_priority_order else -1
    )
    if group['round_rank'].max() == -1:
        return None

    group = group.sort_values(by='parsed_date', ascending=False)
    best = group.iloc[0]
    wins = ", ".join(group['notable_win'].dropna().loc[group['notable_win'] != ""])
    losses = ", ".join(group['lost_to'].dropna().loc[group['lost_to'] != ""])
    return pd.Series({
        "athlete_id": best['athlete_id'],
        "tournament_name": best['tournament_name'],
        "final_position": best['final_position'],
        "tournament_grade": best['tournament_grade'],
        "tournament_year": best['tournament_year'],
        "tournament_date": best['parsed_date'],
        "notable_wins": wins,
        "lost_to": losses
    })

summary_df = (
    filtered_df.groupby(['athlete_id', 'tournament_name'])
    .apply(summarize)
    .dropna()
    .reset_index(drop=True)
)

summary_df['tournament_year'] = summary_df['tournament_year'].astype(int)
summary_df['athlete_id'] = summary_df['athlete_id'].astype(int)
summary_df['tournament_date'] = pd.to_datetime(summary_df['tournament_date'], errors='coerce')

summary_df = summary_df.sort_values(by=['athlete_id', 'tournament_date'], ascending=[True, False])

# Save & insert
def save_and_insert(year, model_class):
    year_df = summary_df[
        (summary_df['tournament_year'] == year) &
        (summary_df['lost_to'].str.strip() != "")
    ].copy()

    year_df['tournament_date'] = year_df['tournament_date'].dt.strftime('%d-%b-%Y')
    year_df.to_csv(f"singles_notable_wins_{year}.csv", index=False)

    records = [
        model_class(
            athlete_id=row['athlete_id'],
            tournament_name=row['tournament_name'],
            final_position=row['final_position'],
            tournament_grade=row['tournament_grade'],
            tournament_year=row['tournament_year'],
            tournament_date=pd.to_datetime(row['tournament_date'], format="%d-%b-%Y"),
            notable_wins=row['notable_wins'],
            lost_to=row['lost_to']
        )
        for _, row in year_df.iterrows()
    ]
    session.add_all(records)
    session.commit()
    print(f"Inserted {len(records)} into {model_class.__tablename__}")

save_and_insert(2024, NotableWins2024)
save_and_insert(2025, NotableWins2025)
session.close()
