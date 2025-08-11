"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String

from orm.archery.base import Base


class BadmintonTournamentProcessing(Base):
    __tablename__ = 'badminton_tournament_details_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer, nullable=False)
    tournament_name = Column(String(255), nullable=False)
    grade = Column(String(255), nullable=True)
    new_grade = Column(String(255), nullable=False)
    date = Column(String(255), nullable=False)
    year = Column(Integer, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date =  Column(Date, nullable=False)
    athlete_id = Column(Integer, nullable=False)
