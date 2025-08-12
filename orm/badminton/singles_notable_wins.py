"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String,Float

from orm.archery.base import Base


class NotableWinsWithRanks(Base):
    __tablename__ = "singles_notable_wins"  # Change to your target table name

    id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer)
    tournament_name = Column(String(255))
    tournament_grade = Column(String(50))
    round_name = Column(String(100))
    athlete_id = Column(Integer)
    athlete_name = Column(String(255))
    opponent_id = Column(Integer)
    opponent_name = Column(String(255))
    win_flag = Column(String(50)) # 0 or 1
    start_date = Column(Date)
    year = Column(Integer)
    athlete_world_ranking = Column(String(50))
    opponent_world_ranking = Column(String(50))