"""
Archery ORM Models

This module defines the SQLAlchemy ORM model for storing average arrow scores of archery athletes
during various phases of competition (qualification, elimination, or overall) in a unified structure
to support visualization and analysis.

Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryArrowAverage(Base):
    """
    ORM model for the 'archery_arrow_average_viz' table.

    This table captures average arrow scores of archery athletes for a specific phase
    (qualification, elimination, or overall) in each competition.

    Attributes:
        id (int): Auto-incremented unique identifier for each record.
        athlete_id (int): Unique ID of the athlete.
        athlete_name (str): Full name of the athlete.
        competition_id (int): Unique ID of the competition.
        comp_name (str): Full or short name of the competition.
        comp_date (date): Date of the competition.
        comp_year (int): Year of the competition.
        type_arrow_avg (str): Type of average ('qualification', 'elimination', 'overall').
        arrow_average (float): Average arrow score for the given type.
    """
    __tablename__ = 'archery_arrow_average_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)

    athlete_id = Column(Integer, nullable=False)
    athlete_name = Column(String(255), nullable=False)

    competition_id = Column(Integer, nullable=False)
    comp_name = Column(String(100), nullable=False)

    comp_date = Column(Date, nullable=True)
    comp_year = Column(Integer, nullable=False)

    type_arrow_avg = Column(String(50), nullable=False)  # e.g., 'qualification', 'elimination', 'overall'
    arrow_average = Column(Float, nullable=True)
