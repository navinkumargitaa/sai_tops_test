"""
This module exposes the SAI database engine for use in other modules.
"""

__author__ = "navin@gitaa.in"

from .database import sai_db_engine

__all__ = ["sai_db_engine"]