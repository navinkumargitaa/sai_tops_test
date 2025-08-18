"""

"""

__author__ = "navin@gitaa.in"

from sqlalchemy import Column, Integer, String

from orm.archery.base import Base


class BadmintonDoublesTournamentFinishes(Base):
    __tablename__ = "z_badminton_doubles_tournament_finishes_viz"

    # Index columns
    athlete_id = Column(Integer, primary_key=True)
    athlete_name = Column(String(255))
    tournament_year = Column(Integer, primary_key=True)

    # Tournament grade-position counts
    Junior_LT_R32 = Column(Integer, default=0)
    Junior_R32 = Column(Integer, default=0)
    Junior_R16 = Column(Integer, default=0)
    Junior_QF = Column(Integer, default=0)
    Junior_SF = Column(Integer, default=0)
    Junior_F = Column(Integer, default=0)

    G3_LT_R32 = Column(Integer, default=0)
    G3_R32 = Column(Integer, default=0)
    G3_R16 = Column(Integer, default=0)
    G3_QF = Column(Integer, default=0)
    G3_SF = Column(Integer, default=0)
    G3_F = Column(Integer, default=0)

    Super100_LT_R32 = Column(Integer, default=0)
    Super100_R32 = Column(Integer, default=0)
    Super100_R16 = Column(Integer, default=0)
    Super100_QF = Column(Integer, default=0)
    Super100_SF = Column(Integer, default=0)
    Super100_F = Column(Integer, default=0)

    Super300_LT_R32 = Column(Integer, default=0)
    Super300_R32 = Column(Integer, default=0)
    Super300_R16 = Column(Integer, default=0)
    Super300_QF = Column(Integer, default=0)
    Super300_SF = Column(Integer, default=0)
    Super300_F = Column(Integer, default=0)

    Super500_LT_R32 = Column(Integer, default=0)
    Super500_R32 = Column(Integer, default=0)
    Super500_R16 = Column(Integer, default=0)
    Super500_QF = Column(Integer, default=0)
    Super500_SF = Column(Integer, default=0)
    Super500_F = Column(Integer, default=0)

    Super750_LT_R32 = Column(Integer, default=0)
    Super750_R32 = Column(Integer, default=0)
    Super750_R16 = Column(Integer, default=0)
    Super750_QF = Column(Integer, default=0)
    Super750_SF = Column(Integer, default=0)
    Super750_F = Column(Integer, default=0)

    Super1000_LT_R32 = Column(Integer, default=0)
    Super1000_R32 = Column(Integer, default=0)
    Super1000_R16 = Column(Integer, default=0)
    Super1000_QF = Column(Integer, default=0)
    Super1000_SF = Column(Integer, default=0)
    Super1000_F = Column(Integer, default=0)

    Grade1_LT_R32 = Column(Integer, default=0)
    Grade1_R32 = Column(Integer, default=0)
    Grade1_R16 = Column(Integer, default=0)
    Grade1_QF = Column(Integer, default=0)
    Grade1_SF = Column(Integer, default=0)
    Grade1_F = Column(Integer, default=0)