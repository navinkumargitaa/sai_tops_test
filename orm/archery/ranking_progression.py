"""
SQLAlchemy ORM model for the 'archery_ranking_progression_viz' table in the database.
This table stores each athlete's ranking progression snapshot as of the end of October each year.

Date: 2025-07-08
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Float, Date, String

from orm.archery.base import Base


class ArcheryRankingProgression(Base):
    """
    ORM model for the 'archery_ranking_progression_viz' table.

    Stores yearly snapshots of athlete rankings and associated points
    as of the end of October for longitudinal performance tracking.

    Attributes:
        id (int): Primary key.
        athlete_id (int): Unique identifier for the athlete.
        year (int): Year of the ranking snapshot.
        rank (int): Rank of the athlete at that time.
        points (float): Points held by the athlete.
        rank_date_issued (date): Date on which the ranking was officially issued.
    """
    __tablename__ = 'archery_ranking_progression_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique record ID

    athlete_id = Column(Integer, nullable=False)  # Athlete's unique ID

    athlete_name = Column(String(255), nullable=False)

    year = Column(Integer, nullable=False)  # Ranking year (e.g., 2023)

    rank = Column(Integer)  # Athlete's rank at end of October

    ranking_status = Column(String(50), nullable=False)  # Athlete's ranking status

    rank_date_issued = Column(Date)  # Date when the rank was published


