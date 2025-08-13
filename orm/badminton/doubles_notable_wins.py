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
    tournament_id = Column(Integer)
    tournament_name = Column(String(255))
    tournament_grade = Column(String(50))
    round_name = Column(String(100))
    athlete_team_name = Column(String(255))
    notabele_win = Column(String(255))  # stores opponent/team beaten
    lost_to = Column(String(255))       # stores opponent/team lost to

    def __repr__(self):
        return (
            f"<DoublesNotableWinLoss(tournament_id={self.tournament_id}, "
            f"athlete_team_name='{self.athlete_team_name}', "
            f"notabele_win='{self.notabele_win}', lost_to='{self.lost_to}')>")