"""
Archery ORM Models

This module defines the SQLAlchemy ORM model for storing average arrow scores of archery athletes
during qualification, elimination, and overall competition rounds.

Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryArrowAverage(Base):
    """
    ORM model for the 'archery_arrow_average_viz' table.

    This table captures average arrow scores of archery athletes in different
    phases of a competition: qualification, elimination, and overall average.

    Attributes:
        id (int): Auto-incremented unique identifier for each record.
        athlete_id (int): Unique ID of the athlete.
        athlete_name (str): Full name of the athlete.
        competition_id (int): Unique ID of the competition.
        comp_new_short_name (str): Short name used for the competition in visualizations.
        qual_avg_arrow (float): Average arrow score in qualification round.
        elem_avg_arrow (float): Average arrow score in elimination round.
        competition_avg_arrow (float): Overall average arrow score across the competition.
    """
    __tablename__ = 'archery_arrow_average_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Primary key for the table

    athlete_id = Column(Integer, nullable=False)  # Athlete's unique identifier

    athlete_name = Column(String(255), nullable=False)  # Athlete's full name

    competition_id = Column(Integer, nullable=False)  # Competition's unique identifier

    comp_name = Column(String(100), nullable=False)  # Short name for competition (used in charts)

    comp_date = Column(Date) # Comp Date

    comp_year = Column(Integer, nullable=False) # Comp Year

    qual_avg_arrow = Column(Float, nullable=True)  # Average arrow score in qualification round

    elem_avg_arrow = Column(Float, nullable=True)  # Average arrow score in elimination round

    competition_avg_arrow = Column(Float, nullable=True)  # Overall average arrow score in the competition
