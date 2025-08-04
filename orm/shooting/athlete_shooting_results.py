"""
Shooting ORM Models

This module defines the SQLAlchemy ORM model for shooting

Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ShootingResultsViz(Base):
    """
    ORM model for shooting results visualization table.
    Stores athlete-level performance, qualification/final ranks,
    and computed q_min/q_max for analysis.
    """
    __tablename__ = 'shooting_results_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Surrogate primary key

    competition_id = Column(String(50), nullable=False)         # Unique ID of the competition
    competition_name = Column(String(255), nullable=False)      # Name of the competition (e.g., World Cup)
    event_name = Column(String(255), nullable=False)            # Specific event name (e.g., 10m Air Rifle Men)

    comp_year = Column(Integer, nullable=False)                 # Year of the competition
    comp_date = Column(Date, nullable=False)                    # Actual date of the competition

    athlete_name = Column(String(255), nullable=False)          # Full name of the athlete
    score = Column(Float)                                       # Qualification round score (as numeric)

    last_attained_rank = Column(Float)                          # Final rank attained by the athlete (from Final or Qualification)
    rank_type = Column(String(50))                              # Indicates if rank was from 'Final' or 'Qualification'

    host_nation = Column(String(100))                           # Host country name
    host_nation_code = Column(String(10))                       # ISO or short code of the host country
    host_city = Column(String(100))                             # City where competition took place

    comp_type = Column(String(100))                             # Type of competition (e.g., World Championships)
    comp_short_name = Column(String(100))
    q_min = Column(Float)                                        # Computed lower threshold for qualification (mean - std dev)
    q_max = Column(Float)