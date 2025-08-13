"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String,Float

from orm.archery.base import Base


class DoublesNotableWinLoss(Base):
    __tablename__ = "z_badminton_doubles_notable_win_loss"

    id = Column(Integer, primary_key=True, autoincrement=True)

    athlete_id = Column(Integer)
    tournament_id = Column(Integer)
    tournament_name = Column(String(255))
    tournament_grade = Column(String(50))
    round_name = Column(String(100))

    athlete_team_name = Column(String(255))
    athlete_team_id = Column(Integer)
    opponent_team_name = Column(String(255))
    opponent_team_id = Column(Integer)

    win_flag = Column(String(50))  # True if win, False if loss
    start_date = Column(Date)
    year = Column(Integer)

    athlete_team_world_ranking = Column(Integer)
    opponent_team_world_ranking = Column(Integer)

    notable_win = Column(String(50))  # True if notable win
    lost_to = Column(String(255))

    def __repr__(self):
        return (
            f"<DoublesNotableWinLoss(id={self.id}, tournament_id={self.tournament_id}, "
            f"athlete_team_name='{self.athlete_team_name}', "
            f"opponent_team_name='{self.opponent_team_name}', "
            f"win_flag={self.win_flag}, notable_win={self.notable_win}, "
            f"lost_to='{self.lost_to}')>"
        )