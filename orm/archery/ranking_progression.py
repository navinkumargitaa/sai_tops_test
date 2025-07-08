"""
archery_ranking_october.py

SQLAlchemy ORM model for the 'archery_ranking_progression_viz' table in the database.
This table stores each athlete's ranking snapshot as of the end of October each year.

Date: 2025-07-08
"""
__author__= "navin@gitaa.in"

from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryRankingProgression(Base):
    """
    ORM model for the 'archery_ranking_october' table.
    Stores an athlete's ranking snapshot as of end of October each year.
    """
    __tablename__ = 'archery_ranking_progression_viz'


    id = Column(Integer, primary_key=True, autoincrement=True)

    # Athlete_ID
    athlete_id = Column(Integer, nullable=False)

    # Ranking year
    year = Column(Integer, nullable=False)

    # Rank of the Athlete
    rank = Column(Integer)

    # Athlete Points
    points = Column(Float)

    # Rank issue date
    rank_date_issued = Column(Date)

    def __repr__(self):
        return (
            f"<ArcheryRankingProgression(id={self.id}, athlete_id={self.athlete_id}, "
            f"year={self.year}, rank={self.rank}, points={self.points}, "
            f"rank_date_issued={self.rank_date_issued})>"
        )
