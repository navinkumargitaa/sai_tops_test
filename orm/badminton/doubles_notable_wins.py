"""
SQLAlchemy ORM model for the 'badminton_doubles_ranking_viz' table.
Stores each athlete's competition ranking snapshot as of the end of October each year.
"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, Date, String,Float

from orm.archery.base import Base


class DoublesNotableWinsWithRanks(Base):
    __tablename__ = "z_badminton_doubles_notable_wins_viz"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(Integer)
    tournament_name = Column(String(255))
    tournament_grade = Column(String(50))
    round_name = Column(String(100))

    athlete_team_name = Column(String(255))
    athlete_team_id = Column(Integer)

    opponent_team_name = Column(String(255))
    opponent_team_id = Column(Integer)

    win_flag = Column(String(50))  # 0 or 1

    start_date = Column(Date)
    week_number = Column(Integer)
    year = Column(Integer)

    athlete_team_id_rank = Column(Integer)
    opponent_team_id_rank = Column(Integer)

    def __repr__(self):
        return (
            f"<DoublesNotableWinsWithRanks(tournament_id={self.tournament_id}, "
            f"athlete_team_name='{self.athlete_team_name}', opponent_team_name='{self.opponent_team_name}', "
            f"win_flag={self.win_flag}, year={self.year})>"
        )