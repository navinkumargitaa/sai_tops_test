

import pandas as pd
# from database import sai_db_engine_badminton,read_h2h_matches_data


sai_db_engine_badminton = "mysql+pymysql://root:harsha123%40@localhost/sai_badminton_viz_final"


def read_h2h_matches_data():

    query = """
    
    SELECT
    a.team_1_athlete_1_id as main_id,
    b.head_to_head_stats_id,
    b.team_1_athlete_1_id,
    b.team_1_athlete_1_name,
    b.team_2_athlete_1_id,
    b.team_2_athlete_1_name,
    b.tournament_id,
    b.tournament_name,
    b.tournament_date,
    b.round_name,
    b.winner
    FROM sai_badminton_viz_final.badminton_h2h_match_stats b
    JOIN
    sai_badminton_viz_final.badminton_h2h_stats a
    ON
    b.head_to_head_stats_id = a.row_id
    ;
    """
    return query
query_matches = read_h2h_matches_data()

h2h_matches = pd.read_sql_query(query_matches,con=sai_db_engine_badminton)

import pandas as pd


# -------------------------------------------------------------------
# df  ▸ your SQL result already loaded as a DataFrame
#      must contain columns:
#      main_id, team_1_athlete_1_id, team_1_athlete_1_name,
#      team_2_athlete_1_id, team_2_athlete_1_name, winner,
#      tournament_id, tournament_name, tournament_date, round_name,
#      head_to_head_stats_id   (plus anything else you queried)
# -------------------------------------------------------------------

def normalize_h2h(df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per unique match with main_id always on the left side."""

    # 1️⃣ keep only rows that involve the main athlete
    df = df.loc[
        (df["main_id"] == df["team_1_athlete_1_id"]) |
        (df["main_id"] == df["team_2_athlete_1_id"])
        ].copy()

    # 2️⃣ compute result (before any swapping)
    df["result_for_main_id"] = df.apply(
        lambda r: "Win" if (
                (r["main_id"] == r["team_1_athlete_1_id"] and r["winner"] == 1) or
                (r["main_id"] == r["team_2_athlete_1_id"] and r["winner"] == 2)
        ) else "Loss",
        axis=1
    )

    # 2️⃣ swap sides when main athlete is in team_2 columns
    swap_mask = df["main_id"] == df["team_2_athlete_1_id"]
    id_cols = ["team_1_athlete_1_id", "team_2_athlete_1_id"]
    name_cols = ["team_1_athlete_1_name", "team_2_athlete_1_name"]

    df.loc[swap_mask, id_cols] = df.loc[swap_mask, id_cols[::-1]].values
    df.loc[swap_mask, name_cols] = df.loc[swap_mask, name_cols[::-1]].values

    # 3️⃣ rename for clarity
    df = df.rename(columns={
        "team_1_athlete_1_id": "main_athlete_id",
        "team_1_athlete_1_name": "main_athlete_name",
        "team_2_athlete_1_id": "opponent_athlete_id",
        "team_2_athlete_1_name": "opponent_athlete_name",
    })



    # 5️⃣ drop true duplicates (same match appearing with different stats IDs)
    key_cols = [
        "main_athlete_id", "opponent_athlete_id",
        "tournament_name", "tournament_date", "round_name"
    ]
    df = (
        df
        .drop_duplicates(subset=key_cols, keep="first")  # keep first copy of each match
        .sort_values("tournament_date", ascending=False)
        .reset_index(drop=True)
    )

    # 6️⃣ tidy column order (optional)
    preferred = [
        "head_to_head_stats_id",
        "main_athlete_id", "main_athlete_name",
        "opponent_athlete_id", "opponent_athlete_name",
        "tournament_id", "tournament_name", "tournament_date",
        "round_name", "result_for_main_id"
    ]
    df = df[[c for c in preferred if c in df.columns] +
            [c for c in df.columns if c not in preferred]]

    return df


# --------------------------- run it ---------------------------
result_df = normalize_h2h(h2h_matches)

# sanity check – any duplicate matches left?
dupe_mask = result_df.duplicated(
    subset=["main_athlete_id", "opponent_athlete_id",
            "tournament_name", "tournament_date", "round_name"],
    keep=False
)
if dupe_mask.any():
    print("⚠️  Warning: some duplicate matches still present")
else:
    print("✅  No duplicate matches remain")

result_df_filtered = result_df[
    ((result_df['main_athlete_id'] == 68870) & (result_df['opponent_athlete_id'] == 25831)) |
    ((result_df['main_athlete_id'] == 25831) & (result_df['opponent_athlete_id'] == 68870))
]

result_df.to_csv("h2h_matches.csv")
new_df = result_df


# List of IDs you want to keep
ids_to_keep = [
    83950, 68870, 73173, 69093, 59687, 74481, 58664, 68322, 99042, 91807, 97168, 70595, 82572,
    72435, 69560, 71612, 57372
]

# Filter the DataFrame
filtered_df = new_df[new_df['main_athlete_id'].isin(ids_to_keep)]
filtered_df.to_csv("h2h_athlete_ids.csv")

# have a look
print(result_df.head())

# # ---------------------------
# # df  ▸ your SQL result set
# # ---------------------------
# def normalize_h2h(df: pd.DataFrame) -> pd.DataFrame:
#     """Orient every row so that `main_id` is shown as the left‑hand athlete."""
#     # 1️⃣ Keep only rows that actually involve the main athlete in that row
#     df = df.loc[
#         (df["main_id"] == df["team_1_athlete_1_id"]) |
#         (df["main_id"] == df["team_2_athlete_1_id"])
#     ].copy()
#
#     # 2️⃣ For rows where the main athlete sits on the right, swap the ID/name pairs
#     mask = df["main_id"] == df["team_2_athlete_1_id"]
#
#     id_cols   = ["team_1_athlete_1_id",   "team_2_athlete_1_id"]
#     name_cols = ["team_1_athlete_1_name", "team_2_athlete_1_name"]
#
#     df.loc[mask, id_cols]   = df.loc[mask, id_cols[::-1]].values
#     df.loc[mask, name_cols] = df.loc[mask, name_cols[::-1]].values
#
#     # 3️⃣ Give the columns friendlier names
#     df = df.rename(columns={
#         "team_1_athlete_1_id":   "main_athlete_id",
#         "team_1_athlete_1_name": "main_athlete_name",
#         "team_2_athlete_1_id":   "opponent_athlete_id",
#         "team_2_athlete_1_name": "opponent_athlete_name"
#     })
#
#     # 4️⃣ Win / Loss from the main athlete’s perspective
#     df["result_for_main_id"] = (
#         df["winner"] == df["main_athlete_id"]
#     ).map({True: "Win", False: "Loss"})
#
#     # 5️⃣ Return whichever columns you need
#     keep = [
#         "head_to_head_stats_id",
#         "main_athlete_id", "main_athlete_name",
#         "opponent_athlete_id", "opponent_athlete_name",
#         "tournament_id", "tournament_name", "tournament_date",
#         "round_name", "result_for_main_id"
#     ]
#     return df[keep].sort_values("tournament_date", ascending=False)
#
# # ---------- Example ----------
# result_df = normalize_h2h(h2h_matches)
#
# key_cols = [
#     "head_to_head_stats_id",
#     "main_athlete_id",
#     "opponent_athlete_id"
# ]
#
# # keep only one row per match; `last` keeps the row with the latest winner flag
# result_df = (
#     normalize_h2h(h2h_matches)
#       .sort_values("tournament_date")          # optional: chronological order
#       .drop_duplicates(subset=key_cols, keep="last")
#       .reset_index(drop=True)
# )
#
#
# result_df_68870 = result_df[result_df['main_athlete_id']==68870]
#
# print(result_df)
#
