
"""

"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String,Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ArcheryArrowAverage(Base):
    """
    ORM model for the 'archery_competition_ranking_viz' table.
    Stores an athlete's performance in terms of average arrow scores.
    """
    __tablename__ = 'archery_arrow_average_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Athlete ID
    athlete_id = Column(Integer, nullable=False)

    # Athlete Name
    athlete_name = Column(String(255), nullable=False)

    # Competition ID
    competition_id = Column(Integer, nullable=False)

    # New short name for the competition
    comp_new_short_name = Column(String(100), nullable=False)

    # Qualification round average arrow score
    qual_avg_arrow = Column(Float, nullable=True)

    # Elimination round average arrow score
    elem_avg_arrow = Column(Float, nullable=True)

    # Combined competition average arrow score
    competition_avg_arrow = Column(Float, nullable=True)

