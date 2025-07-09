# services/tournament_analysis.py

import pandas as pd
from model.badminton.database import SessionLocal
from orm.badminton.singles_tournament_finishes import TournamentFinish2024, TournamentFinish2025


def generate_tournament_summary(df, years=[2024, 2025]):
    grade_levels = ["100", "300", "500", "750", "1000", "G1_CC"]
    position_stages = ["R32", "R16", "QF", "SF", "F"]

    df = df.rename(columns={"tournament_grade": "grade", "final_position": "position"})
    df["grade"] = df["grade"].fillna("G1_CC").astype(str)
    df = df[df["tournament_year"].isin(years)]

    result = {}

    for year in years:
        summaries = []
        df_year = df[df["tournament_year"] == year]

        for aid in df_year["athlete_id"].unique():
            sub = df_year[df_year["athlete_id"] == aid]
            pivot = sub.pivot_table(index="grade", columns="position", aggfunc='size', fill_value=0)
            pivot = pivot.reindex(index=grade_levels, columns=position_stages).fillna(0).astype(int)

            summary = {"athlete_id": aid}
            for pos in position_stages:
                for grade in grade_levels:
                    col = f"{pos}_Super_{grade}"
                    summary[col] = pivot.at[grade, pos] if pivot.at[grade, pos] != 0 else ""
            summaries.append(summary)

        result[year] = pd.DataFrame(summaries)

    return result


def save_summary_to_db(df_dict):
    session = SessionLocal()
    mapping = {
        2024: TournamentFinish2024,
        2025: TournamentFinish2025
    }

    for year, df in df_dict.items():
        model_cls = mapping[year]
        records = [model_cls(**row) for _, row in df.fillna("").iterrows()]
        session.add_all(records)
        print(f"Inserting {len(records)} rows into `{model_cls.__tablename__}`...")
    session.commit()
    session.close()
