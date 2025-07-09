

__author__= "navin@gitaa.in"

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TournamentFinishBase(Base):
    __abstract__ = True
    athlete_id = Column(Integer, primary_key=True)

    for pos in ["R32", "R16", "QF", "SF", "F"]:
        for grade in ["100", "300", "500", "750", "1000", "G1_CC"]:
            col_name = f"{pos}_Super_{grade}"
            vars()[col_name] = Column(String(10))  # Stores count or blank


class TournamentFinish2024(TournamentFinishBase):
    __tablename__ = "badminton_singles_tournament_finishes_2024"


class TournamentFinish2025(TournamentFinishBase):
    __tablename__ = "badminton_singles_tournament_finishes_2025"