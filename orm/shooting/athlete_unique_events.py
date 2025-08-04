"""
SAI TOPS : Shooting : Visualization : Athlete unique events
=================================================================
This module defines the cleaned and standardized ORM model for
visualization of shooting athletes after merging and preprocessing.
"""

from __future__ import annotations
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR , INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

__author__ = "navin@gitaa.in"

class ShootingAthleteEvents(Base):
    """
    ORM class for shooting athlete events
    """

    __tablename__ = "shooting_athlete_events"

    id = Column(INTEGER, primary_key=True, autoincrement=True)

    #: Athlete name
    athlete_name = Column(VARCHAR(255), nullable=True, comment="Athlete name")

    #: Gender
    athlete_event = Column(VARCHAR(255), nullable=True, comment="Athlete event")