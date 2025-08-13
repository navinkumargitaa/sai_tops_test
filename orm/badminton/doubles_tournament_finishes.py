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
    Junior_LT_R32 = Column("Junior_<R32", Integer, default=0)
    Junior_R32 = Column(Integer, default=0)
    Junior_R16 = Column(Integer, default=0)
    Junior_QF = Column(Integer, default=0)
    Junior_SF = Column(Integer, default=0)
    Junior_F = Column(Integer, default=0)

    G3_LT_R32 = Column("G3_<R32", Integer, default=0)
    G3_R32 = Column(Integer, default=0)
    G3_R16 = Column(Integer, default=0)
    G3_QF = Column(Integer, default=0)
    G3_SF = Column(Integer, default=0)
    G3_F = Column(Integer, default=0)

    g100_LT_R32 = Column("100_<R32", Integer, default=0)
    g100_R32 = Column(Integer, default=0)
    g100_R16 = Column(Integer, default=0)
    g100_QF = Column(Integer, default=0)
    g100_SF = Column(Integer, default=0)
    g100_F = Column(Integer, default=0)

    g300_LT_R32 = Column("300_<R32", Integer, default=0)
    g300_R32 = Column(Integer, default=0)
    g300_R16 = Column(Integer, default=0)
    g300_QF = Column(Integer, default=0)
    g300_SF = Column(Integer, default=0)
    g300_F = Column(Integer, default=0)

    g500_LT_R32 = Column("500_<R32", Integer, default=0)
    g500_R32 = Column(Integer, default=0)
    g500_R16 = Column(Integer, default=0)
    g500_QF = Column(Integer, default=0)
    g500_SF = Column(Integer, default=0)
    g500_F = Column(Integer, default=0)

    g750_LT_R32 = Column("750_<R32", Integer, default=0)
    g750_R32 = Column(Integer, default=0)
    g750_R16 = Column(Integer, default=0)
    g750_QF = Column(Integer, default=0)
    g750_SF = Column(Integer, default=0)
    g750_F = Column(Integer, default=0)

    g1000_LT_R32 = Column("1000_<R32", Integer, default=0)
    g1000_R32 = Column(Integer, default=0)
    g1000_R16 = Column(Integer, default=0)
    g1000_QF = Column(Integer, default=0)
    g1000_SF = Column(Integer, default=0)
    g1000_F = Column(Integer, default=0)

    grade1_LT_R32 = Column("Grade 1_<R32", Integer, default=0)
    grade1_R32 = Column(Integer, default=0)
    grade1_R16 = Column(Integer, default=0)
    grade1_QF = Column(Integer, default=0)
    grade1_SF = Column(Integer, default=0)
    grade1_F = Column(Integer, default=0)