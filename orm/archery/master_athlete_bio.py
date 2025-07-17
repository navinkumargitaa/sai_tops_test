"""
SAI TOPS : Archery : Visualization : Athlete Final Cleaned View
=================================================================
This module defines the cleaned and standardized ORM model for
visualization of archery athletes after merging and preprocessing.
"""

from __future__ import annotations
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import BIGINT, VARCHAR, DATE
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

__author__ = "navin@gitaa.in"

class ArcheryAthleteFinalViz(Base):
    """
    ORM class for cleaned archery athlete bio details
    """

    __tablename__ = "archery_athlete_final_viz"


    #: Athlete ID
    athlete_id = Column(BIGINT, primary_key=True, index=True, comment="Unique athlete ID")

    #: Athlete name
    name = Column(VARCHAR(255), nullable=True, comment="Athlete name")

    #: Gender
    gender = Column(VARCHAR(50), nullable=True, comment="Gender")

    #: Date of Birth
    dob = Column(DATE, nullable=True, comment="Date of birth")

    #: Age
    age = Column(BIGINT(unsigned=True), nullable=True, comment="Athlete age")

    #: City
    city = Column(VARCHAR(100), nullable=True, comment="City of athlete")

    #: Domicile State
    domicile_state = Column(VARCHAR(100), nullable=True, comment="Domicile state")

    #: Country
    country = Column(VARCHAR(100), nullable=True, comment="Country of athlete")

    #: Profile Picture URL
    profile_picture_url = Column(VARCHAR(255), nullable=True, comment="Profile picture URL")

    #: Event
    event = Column(VARCHAR(255), nullable=True, comment="Event")

    #: Current Group
    current_group = Column(VARCHAR(100), nullable=True, comment="Core / Development group")

    #: Training Base
    training_base = Column(VARCHAR(255), nullable=True, comment="Training base name")

    #: Supported by NGO
    supported_by_ngo = Column(VARCHAR(255), nullable=True, comment="NGO support info")

    #: Employer
    employer = Column(VARCHAR(255), nullable=True, comment="Employer")

    #: Support Till Date
    support_till_date = Column(VARCHAR(100), nullable=True, comment="Support till date")

    #: Raw Induction Text
    raw_induction_text = Column(VARCHAR(1000), nullable=True, comment="Raw induction string")

    #: Transfer to Core Date
    transfer_to_core_date = Column(DATE, nullable=True, comment="Date of transfer from development to core")

    #: Current Induction Date
    current_induction_date = Column(DATE, nullable=True, comment="Current induction date")

    #: Current MOC Meeting Number
    current_moc_meeting_no = Column(VARCHAR(100), nullable=True, comment="Current MOC Meeting number")

    #: First Induction
    first_induction = Column(DATE, nullable=True, comment="First induction date")

    #: First Exclusion
    first_exclusion = Column(DATE, nullable=True, comment="First exclusion date")

    #: Second Inclusion
    second_inclusion = Column(DATE, nullable=True, comment="Second inclusion date")

    #: Coach
    coach = Column(VARCHAR(255), nullable=True, comment="Coach name")

    #: Physiotherapist
    physio = Column(VARCHAR(255), nullable=True, comment="Physiotherapist")

    #: Strength & Conditioning
    strength_conditioning = Column(VARCHAR(255), nullable=True, comment="Strength & Conditioning expert")

    #: Nutritionist
    nutritionist = Column(VARCHAR(255), nullable=True, comment="Nutritionist")

    #: Psychologist
    psychologist = Column(VARCHAR(255), nullable=True, comment="Psychologist")

    #: Masseur
    masseur = Column(VARCHAR(255), nullable=True, comment="Masseur")