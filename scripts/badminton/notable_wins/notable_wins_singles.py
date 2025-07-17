import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Connect to MySQL
engine = create_engine("mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final")

# Load tournament match data
tournament_df = pd.read_sql("""
SELECT DISTINCT 
  a.athlete_id,
  a.tournament_id,
  round_name,
  draw_name_full,
  winner,
  team_1_player_1_id,
  team_1_player_1_name,
  team_2_player_1_id,
  team_2_player_1_name,
  name AS tournament_name,
  grade AS tournament_grade,
  date,
  year
FROM sai_badminton_viz_final.badminton_athlete_match a
JOIN sai_badminton_viz_final.badminton_athlete_tournament b
ON a.tournament_id = b.tournament_id;
""", con=engine)

# Filter only for selected athletes
selected_athlete_ids = [83950, 68870, 73173, 69093, 59687, 74481, 58664, 68322, 99042, 91807, 97168, 70595, 82572]
tournament_df = tournament_df[tournament_df['athlete_id'].isin(selected_athlete_ids)]

# Load and clean ranking data
ranking_df = pd.read_sql("SELECT player_id, `rank` FROM badminton_singles_rankings", con=engine)
athlete_rank_map = dict(zip(ranking_df['player_id'], ranking_df['rank']))

# Convert date to end format (e.g., "03 - 08 June" → "8 June")
def convert_date(date_text):
    try:
        end_part = date_text.split("-")[1].strip()
        day, month = end_part.split()
        return str(int(day)) + " " + month
    except:
        return date_text

tournament_df["tournament_date"] = tournament_df["date"].apply(convert_date)

# Fix data types
tournament_df['athlete_id'] = tournament_df['athlete_id'].astype(int)
tournament_df['team_1_player_1_id'] = pd.to_numeric(tournament_df['team_1_player_1_id'], errors='coerce').astype('Int64')
tournament_df['team_2_player_1_id'] = pd.to_numeric(tournament_df['team_2_player_1_id'], errors='coerce').astype('Int64')

# Determine win/loss and notable opponents
def determine_win_loss(row):
    athlete_id = row['athlete_id']
    player1_id = row['team_1_player_1_id']
    player2_id = row['team_2_player_1_id']
    winner_side = str(row['winner'])
    player1_name = row['team_1_player_1_name']
    player2_name = row['team_2_player_1_name']

    # If either player ID is missing, skip the row
    if pd.isna(player1_id) or pd.isna(player2_id):
        return pd.Series(["Unknown", "Unknown", "", ""])

    # Convert only if not null
    try:
        player1_id = int(player1_id)
        player2_id = int(player2_id)
    except ValueError:
        return pd.Series(["Unknown", "Unknown", "", ""])

    # Get ranks
    rank1 = athlete_rank_map.get(player1_id)
    rank2 = athlete_rank_map.get(player2_id)

    if athlete_id == player1_id:
        athlete_side = '1'
        athlete_name = player1_name
        opponent_name = player2_name
        athlete_rank = rank1
        opponent_rank = rank2
    elif athlete_id == player2_id:
        athlete_side = '2'
        athlete_name = player2_name
        opponent_name = player1_name
        athlete_rank = rank2
        opponent_rank = rank1
    else:
        return pd.Series(["Unknown", "Unknown", "", ""])

    athlete_display = f"{athlete_name} ({athlete_rank})" if athlete_rank else athlete_name
    opponent_display = f"{opponent_name} ({opponent_rank})" if opponent_rank else opponent_name

    if winner_side == athlete_side:
        notable_win = opponent_display if athlete_rank and opponent_rank and opponent_rank < athlete_rank else ""
        return pd.Series([athlete_display, opponent_display, notable_win, ""])
    else:
        return pd.Series([opponent_display, athlete_display, "", opponent_display])


tournament_df[['win', 'loss', 'notable_win', 'lost_to']] = tournament_df.apply(determine_win_loss, axis=1)

# Fill missing tournament grade
tournament_df['tournament_grade'] = tournament_df['tournament_grade'].fillna("G1_CC")

# Round ranking 
round_order = {'R32': 1, 'R16': 2, 'QF': 3, 'SF': 4, 'F': 5}
tournament_df['round_rank'] = 0
for round_name, rank_value in round_order.items():
    tournament_df.loc[tournament_df['round_name'] == round_name, 'round_rank'] = rank_value

# Determine main athlete name based on team ID
def get_main_athlete_name(row):
    try:
        if row['athlete_id'] == row['team_1_player_1_id']:
            return row['team_1_player_1_name']
        elif row['athlete_id'] == row['team_2_player_1_id']:
            return row['team_2_player_1_name']
    except:
        pass
    return "UNKNOWN"



tournament_df["main_athlete_name"] = tournament_df.apply(get_main_athlete_name, axis=1)

# Grouping: combine notable wins/losses
def combine_names(series):
    return ", ".join(sorted(set(name for name in series if name)))

summary_df = (
    tournament_df.sort_values("round_rank", ascending=False)
    .groupby(["athlete_id", "main_athlete_name", "tournament_id", "tournament_name", "year", "tournament_date"])
    .agg({
        "round_name": "first",
        "notable_win": combine_names,
        "lost_to": combine_names
    })
    .reset_index()
)

# Create datetime column for sorting
def parse_tournament_date(row):
    try:
        return datetime.strptime(f"{row['tournament_date']} {row['year']}", "%d %B %Y")
    except:
        return pd.NaT

summary_df["parsed_date"] = summary_df.apply(parse_tournament_date, axis=1)

# Save 2024 summary
summary_2024 = summary_df[summary_df["year"] == 2024].sort_values(by=["athlete_id", "parsed_date"], ascending=[True, False])
if not summary_2024.empty:
    summary_2024.drop(columns=["parsed_date"]).to_csv("notable_wins_singles_2024.csv", index=False)
    print(" Saved: tournament_summary_2024.csv")

# Save 2025 summary
summary_2025 = summary_df[summary_df["year"] == 2025].sort_values(by=["athlete_id", "parsed_date"], ascending=[True, False])
if not summary_2025.empty:
    summary_2025.drop(columns=["parsed_date"]).to_csv("notable_wins_singles_2025.csv", index=False)
    print(" Saved: tournament_summary_2025.csv")
