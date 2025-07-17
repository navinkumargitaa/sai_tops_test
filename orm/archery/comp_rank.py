"""
SQLAlchemy ORM model for the 'archery_competition_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String

from orm.archery.base import Base

class ArcheryCompetitionRanking(Base):
    __tablename__ = 'archery_competition_ranking_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)
    athlete_id = Column(Integer, nullable=False)
    athlete_name = Column(String(255), nullable=False)
    comp_id = Column(Integer, nullable=False)
    comp_full_name = Column(String(255), nullable=False)
    comp_short_name = Column(String(255), nullable=False)
    comp_place = Column(String(100), nullable=False)
    comp_date = Column(Date)
    comp_rank = Column(Integer)
