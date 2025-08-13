"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String,Float

from orm.archery.base import Base


class NotableWinsSinglesFinal(Base):
    __tablename__ = "z_badminton_singles_notable_wins"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer)
    tournament_name = Column(String(255))
    tournament_grade = Column(String(50))
    round_name = Column(String(100))

    athlete_id = Column(Integer)
    athlete_name = Column(String(255))
    opponent_id = Column(Integer)
    opponent_name = Column(String(255))

    win_flag = Column(String(50))  # 1 = win, 0 = loss
    start_date = Column(Date)
    year = Column(Integer)

    athlete_world_ranking = Column(Integer)
    opponent_world_ranking = Column(Integer)

    notable_win = Column(String(50))  # True if notable win, else False
    lost_to = Column(String(255))

    def __repr__(self):
        return (
            f"<TournamentMatch(tournament_id={self.tournament_id}, "
            f"athlete_id={self.athlete_id}, opponent_id={self.opponent_id}, "
            f"win_flag={self.win_flag})>"
        )