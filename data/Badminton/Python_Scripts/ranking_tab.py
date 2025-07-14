import mysql.connector
import pandas as pd

# Database connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harsha123@",
    database="sai_badminton_viz_final"
)

# SQL query
query = """
SELECT athlete_id, career_best_ranking, latest_ranking 
FROM sai_badminton_viz_final.badminton_ind_bwf_ranking_viz;
"""

# Read data into a DataFrame
df = pd.read_sql(query, conn)

# Convert columns to nullable integer types (Int64 supports NaNs)
df = df.astype({
    "athlete_id": "Int64",
    "career_best_ranking": "Int64",
    "latest_ranking": "Int64"
})

# Save to CSV
df.to_csv("rankings_tab.csv", index=False)

# Close connection
conn.close()

print("Data saved to bwf_rankings.csv with integer values (NaNs preserved)")
