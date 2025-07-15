import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Connect to MySQL
db_url = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"
engine = create_engine(db_url)

# Load all doubles ranking data
ranking_df = pd.read_sql("SELECT * FROM sai_badminton_viz_final.doubles_rankings", engine)
ranking_df = ranking_df.dropna(subset=['player1_id', 'player2_id', 'rank'])

# Load tournament match data
match_df = pd.read_sql("""
SELECT DISTINCT 
    a.row_id AS team_id,
    c.athlete_id,
    c.tournament_id,
    c.round_name,
    c.winner,
    c.team_1_player_1_id,
    c.team_1_player_2_id,
    c.team_2_player_1_id,
    c.team_2_player_2_id,
    c.team_1_player_1_name,
    c.team_1_player_2_name,
    c.team_2_player_1_name,
    c.team_2_player_2_name,
    c.draw_name_full AS draw_name_full,
    d.name AS tournament_name,
    d.date AS date,
    d.grade AS tournament_grade,
    d.year AS year
FROM sai_badminton_viz_final.badminton_doubles a
JOIN sai_badminton_viz_final.badminton_athlete_match c ON a.athlete_1_id = c.athlete_id
JOIN sai_badminton_viz_final.badminton_athlete_tournament d ON c.tournament_id = d.tournament_id;
""",con = engine)

# Filter only selected team IDs
selected_team_ids = [1, 2, 61, 116]
match_df = match_df[match_df['team_id'].isin(selected_team_ids)]

# Helper to get rank
def get_team_rank(p1, p2):
    row = ranking_df[((ranking_df['player1_id'] == p1) & (ranking_df['player2_id'] == p2)) |
                     ((ranking_df['player1_id'] == p2) & (ranking_df['player2_id'] == p1))]
    return row['rank'].iloc[0] if not row.empty else None

# Convert tournament date to readable format (e.g., "03 - 08 June" → "8 June")
def convert_date(date_text):
    try:
        end_part = date_text.split("-")[1].strip()
        day, month = end_part.split()
        return str(int(day)) + " " + month
    except:
        return date_text

match_df["tournament_date"] = match_df["date"].apply(convert_date)

# Fix data types
for col in ['athlete_id', 'team_1_player_1_id', 'team_1_player_2_id', 'team_2_player_1_id', 'team_2_player_2_id']:
    match_df[col] = pd.to_numeric(match_df[col], errors='coerce').astype('Int64')

# Determine win/loss
def determine_win_loss(row):
    athlete_id = row['athlete_id']
    winner = row['winner']
    t1 = [row['team_1_player_1_id'], row['team_1_player_2_id']]
    t2 = [row['team_2_player_1_id'], row['team_2_player_2_id']]

    team1_name = f"{row['team_1_player_1_name']} & {row['team_1_player_2_name']}"
    team2_name = f"{row['team_2_player_1_name']} & {row['team_2_player_2_name']}"

    team1_rank = get_team_rank(*t1)
    team2_rank = get_team_rank(*t2)

    if athlete_id in t1:
        athlete_team = 1
        athlete_name, opponent_name = team1_name, team2_name
        athlete_rank, opponent_rank = team1_rank, team2_rank
    elif athlete_id in t2:
        athlete_team = 2
        athlete_name, opponent_name = team2_name, team1_name
        athlete_rank, opponent_rank = team2_rank, team1_rank
    else:
        return pd.Series(["Unknown", "Unknown", "", ""])

    athlete_display = f"{athlete_name} ({athlete_rank})" if athlete_rank else athlete_name
    opponent_display = f"{opponent_name} ({opponent_rank})" if opponent_rank else opponent_name

    if winner == athlete_team:
        notable_win = opponent_display if opponent_rank and athlete_rank and opponent_rank < athlete_rank else ""
        return pd.Series([athlete_display, opponent_display, notable_win, ""])
    else:
        return pd.Series([opponent_display, athlete_display, "", opponent_display])

match_df[['win', 'loss', 'notable_win', 'lost_to']] = match_df.apply(determine_win_loss, axis=1)

# Fill missing grade
match_df['tournament_grade'] = match_df['tournament_grade'].fillna("G1_CC")

# Round ranking
round_order = {'R32': 1, 'R16': 2, 'QF': 3, 'SF': 4, 'F': 5}
match_df['round_rank'] = 0
for round_name, rank_value in round_order.items():
    match_df.loc[match_df['round_name'] == round_name, 'round_rank'] = rank_value

# Main athlete label based on team

def get_main_athlete_name(row):
    athlete_id = row['athlete_id']
    if athlete_id in [row['team_1_player_1_id'], row['team_1_player_2_id']]:
        return row['team_1_player_1_name'] + ' / ' + row['team_1_player_2_name']
    elif athlete_id in [row['team_2_player_1_id'], row['team_2_player_2_id']]:
        return row['team_2_player_1_name'] + ' / ' + row['team_2_player_2_name']
    return "UNKNOWN"

match_df['main_athlete_name'] = match_df.apply(get_main_athlete_name, axis=1)

# Combine names
def combine_names(series):
    return ", ".join(sorted(set(name for name in series if name)))

# Summarize
summary_df = (
    match_df.sort_values("round_rank", ascending=False)
    .groupby(["team_id", "main_athlete_name", "tournament_id", "tournament_name", "year", "tournament_date"])
    .agg({
        "round_name": "first",
        "notable_win": combine_names,
        "lost_to": combine_names
    })
    .reset_index()
)

# Parse datetime using normal function
def parse_date(row):
    try:
        return datetime.strptime(f"{row['tournament_date']} {row['year']}", "%d %B %Y")
    except:
        return pd.NaT

summary_df["parsed_date"] = summary_df.apply(parse_date, axis=1)

# Save 2024
summary_2024 = summary_df[summary_df["year"] == 2024].sort_values(by=["team_id", "parsed_date"], ascending=[True, False])
if not summary_2024.empty:
    summary_2024.drop(columns=["parsed_date"]).to_csv("tournament_summary_doubles_2024.csv", index=False)
    print("Saved: tournament_summary_doubles_2024.csv")

# Save 2025
summary_2025 = summary_df[summary_df["year"] == 2025].sort_values(by=["team_id", "parsed_date"], ascending=[True, False])
if not summary_2025.empty:
    summary_2025.drop(columns=["parsed_date"]).to_csv("tournament_summary_doubles_2025.csv", index=False)
    print("Saved: tournament_summary_doubles_2025.csv")
