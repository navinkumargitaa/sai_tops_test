"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String

from orm.archery.base import Base

class BadmintonDoublesRanking(Base):
    __tablename__ = 'badminton_doubles_ranking_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)

    athlete_1_id = Column(Integer, nullable=True)
    athlete_1_name = Column(String(255), nullable=True)

    athlete_2_id = Column(Integer, nullable=True)
    athlete_2_name = Column(String(255), nullable=True)

    composite_team_id = Column(String(255), nullable=True)
    team_display_name = Column(String(255), nullable=True)

    ranking_date = Column(Date, nullable=False)
    ranking_year = Column(Integer, nullable=True)
    world_ranking = Column(Integer, nullable=True)
    world_tour_ranking = Column(Integer, nullable=True)