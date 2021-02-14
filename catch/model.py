# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = [
    'CatchQuery',
    'Caught',
    'Observation',
    'Ephemeris',
    'Found',
    'Obj'
]

from sqlalchemy import Column, Integer, Integer, Float, String, ForeignKey
from sbsearch.model import *


class NEATPalomarTricam(Observation):
    __tablename__ = 'neat_palomar_tricam'
    id = Column(Integer, primary_key=True)
    observation_id = Column(
        Integer, ForeignKey('observation.observation_id', onupdate='CASCADE',
                            ondelete='CASCADE'), index=True)
    product_id = Column(String(64), doc='Archive product id',
                        unique=True, index=True)
    instrument = Column(String(64), doc='Instrument / detector name')
    __mapper_args__ = {
        'polymorphic_identity': 'neat_palomar_tricam'
    }
    __data_source_name__ = 'NEAT Palomar Tricam'
    __obscode__ = '644'


class NEATMauiGEODSS(Observation):
    __tablename__ = 'neat_maui_geodss'
    id = Column(Integer, primary_key=True)
    observation_id = Column(
        Integer, ForeignKey('observation.observation_id', onupdate='CASCADE',
                            ondelete='CASCADE'), index=True)
    product_id = Column(String(64), doc='Archive product id',
                        unique=True, index=True)
    instrument = Column(String(64), doc='Instrument / detector name')
    __mapper_args__ = {
        'polymorphic_identity': 'neat_maui_geodss'
    }
    __data_source_name__ = 'NEAT Maui GEODSS'
    __obscode__ = '566'


class SkyMapper(Observation):
    __tablename__ = 'skymapper'
    id = Column(Integer, primary_key=True)
    observation_id = Column(Integer, ForeignKey('observation.observation_id', onupdate='CASCADE',
                                                ondelete='CASCADE'), index=True)
    product_id = Column(String(64), doc='Archive product id',
                        unique=True, index=True)
    sb_mag = Column(Float(16), doc='Surface brightness estimate (ABmag)')
    field_id = Column(Integer, doc='Field ID')
    image_type = Column(String(
        2), doc='Type of image: fs=Fast Survey, ms=Main Survey, std=Standard Field (images)')
    zpapprox = Column(
        Float(16), doc='Approximate photometric zeropoint for the exposure')
    __mapper_args__ = {
        'polymorphic_identity': 'skymapper'
    }
    __data_source_name__ = 'SkyMapper'
    __obscode__ = '413'


class CatchQuery(Base):
    __tablename__ = 'catch_query'
    query_id = Column(Integer, primary_key=True)
    job_id = Column(String(32), index=True,
                    doc="Unique job ID, UUID version 4")
    query = Column(String(128), index=True, doc="User's query string")
    source = Column(String(128), doc="Survey source table queried")
    date = Column(String(64), doc="Date query was executed")
    status = Column(String(64), doc="query status")


class Caught(Base):
    __tablename__ = 'caught'
    query_id = Column(
        Integer, ForeignKey('catch_query.query_id',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
        primary_key=True)
    found_id = Column(
        Integer, ForeignKey('found.found_id',
                            onupdate='CASCADE',
                            ondelete='CASCADE'),
        primary_key=True)
