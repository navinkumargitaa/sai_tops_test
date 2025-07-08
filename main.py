
import pandas as pd
from orm import ArcheryRankingOctober, Base
from model import sai_db_engine
from services.ranking_processing import get_end_of_october_ranking
from sqlalchemy.orm import sessionmaker

# Create table
Base.metadata.create_all(sai_db_engine)

print("Table created successfully.")

# Extract & transform
end_of_october_ranking = get_end_of_october_ranking()
print(end_of_october_ranking.head())

# Load into database
Session = sessionmaker(bind=sai_db_engine)
session = Session()

records = [
    ArcheryRankingOctober(
        athlete_id=int(row['athlete_id']),
        year=int(row['year']),
        rank=int(row['rank']) if pd.notnull(row['rank']) else None,
        points=float(row['points']) if pd.notnull(row['points']) else None,
        rank_date_issued=row['rank_date_issued'].date() if pd.notnull(row['rank_date_issued']) else None
    )
    for idx, row in end_of_october_ranking.iterrows()
]

session.add_all(records)
session.commit()
session.close()