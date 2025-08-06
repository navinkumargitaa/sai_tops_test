"""
Shooting ORM Models

This module defines the SQLAlchemy ORM model for shooting

Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"


from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AthleteRankCategoryViz(Base):
    """
    ORM model to store long-format athlete rank distribution data for visualization.
    Each row captures the count for one category (Top 3, Top 8, Not in Top 8) for a given athlete.
    Suitable for pie/bar chart rendering.
    """
    __tablename__ = 'athlete_rank_category_viz'

    id = Column(Integer, primary_key=True, autoincrement=True)     # Surrogate key
    athlete_name = Column(String(255), nullable=False)             # Full name of the athlete
    event_name = Column(String(50), nullable=False)
    category = Column(String(50), nullable=False)                  # Rank category (e.g., 'top_3', 'top_8', 'not_in_top_8')
    value = Column(Integer, nullable=False, default=0)             # Count of appearances in this category
