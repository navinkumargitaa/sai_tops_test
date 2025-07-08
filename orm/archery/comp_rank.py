"""
archery_ranking_october.py

SQLAlchemy ORM model for the 'competition_ranking_viz' table in the database.
This table stores each athlete's ranking snapshot as of the end of October each year.

Date: 2025-07-08
"""
__author__= "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryCompetitionRanking(Base):
    """
    ORM model for the 'archery_ranking_october' table.
    Stores an athlete's ranking snapshot as of end of October each year.
    """
    __tablename__ = 'archery_competition_ranking_viz'


    id = Column(Integer, primary_key=True, autoincrement=True)

    # Athlete ID
    athlete_id = Column(Integer, nullable=False)

    # Competition ID
    comp_id = Column(Integer, nullable=False)

    # Competition full name
    comp_full_name = Column(String(255), nullable=False)

    # Competition full name
    comp_short_name = Column(String(100), nullable=False)

    # Competition full name
    comp_new_short_name = Column(String(100), nullable=False)

    # Competition place
    comp_place = Column(String(100), nullable=False)

    # Rank issue date
    comp_date = Column(Date)

    # Competition short name
    comp_rank = Column(Integer)



    # def __repr__(self):
    #     return (
    #         f"<ArcheryRankingProgression(id={self.id}, athlete_id={self.athlete_id}, "
    #         f"year={self.year}, rank={self.rank}, points={self.points}, "
    #         f"rank_date_issued={self.rank_date_issued})>"
    #     )
