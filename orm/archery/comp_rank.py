"""
archery_ranking_october.py

SQLAlchemy ORM model for the 'archery_competition_ranking_viz' table in the database.
This table stores each athlete's ranking snapshot as of the end of October each year.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryCompetitionRanking(Base):
    """
    ORM model for the 'archery_competition_ranking_viz' table.

    Stores a snapshot of each athlete's competition ranking as of the end of October
    every year. Useful for seasonal comparisons and performance tracking.

    Attributes:
        id (int): Primary key.
        athlete_id (int): Unique ID of the athlete.
        comp_id (int): Unique ID of the competition.
        comp_full_name (str): Full name of the competition.
        comp_short_name (str): Original short name of the competition.
        comp_new_short_name (str): Cleaned/renamed short name for visualization.
        comp_place (str): Location where the competition was held.
        comp_date (date): Date associated with the ranking.
        comp_rank (int): Athlete's rank in the competition.
    """
    __tablename__ = 'archery_competition_ranking_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique record identifier

    athlete_id = Column(Integer, nullable=False)  # Athlete's unique identifier

    comp_id = Column(Integer, nullable=False)  # Unique ID of the competition

    comp_full_name = Column(String(255), nullable=False)  # Full name of the competition

    comp_short_name = Column(String(100), nullable=False)  # Short name (possibly from source)

    comp_new_short_name = Column(String(100), nullable=False)  # Cleaned short name for charts

    comp_place = Column(String(100), nullable=False)  # Location of the competition

    comp_date = Column(Date)  # Date when the rank was issued

    comp_rank = Column(Integer)  # Athlete's rank in the competition
