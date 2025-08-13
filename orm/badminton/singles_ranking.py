"""
SQLAlchemy ORM model for the 'badminton_singles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String

from orm.archery.base import Base

class BadmintonSinglesRanking(Base):
    __tablename__ = 'z_badminton_singles_ranking_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(Integer, nullable=False)
    athlete_name = Column(String(255), nullable=False)
    ranking_date = Column(Date, nullable=False)
    ranking_year = Column(Integer, nullable=True)
    world_ranking = Column(Integer, nullable=True)
    world_tour_ranking = Column(Integer, nullable=True)

