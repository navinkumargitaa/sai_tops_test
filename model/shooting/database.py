"""
SAI Shooting Data Module

This module provides functions to generate SQL queries for retrieving shooting-related
athlete performance data from the SAI database.
Author: navin@gitaa.in
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import create_engine

# SQLAlchemy engine for connecting to the local SAI database
sai_db_engine = create_engine(url="mysql+pymysql://root:root@localhost:3306/sai_shooting_final")