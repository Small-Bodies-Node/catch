# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = [
    'CatchQuery',
    'Caught',
    'Observation',
    'Ephemeris',
    'Found',
    'Obj',
    'NEATPalomarTricam',
    'NEATMauiGEODSS',
    'SkyMapper',
]

from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sbsearch.model.core import (Base, Observation, Found, Ephemeris, Obj,
                                 BigIntegerType)
from sbsearch.model.example_survey import ExampleSurvey
from sqlalchemy.sql.sqltypes import Boolean
from .neat_palomar_tricam import NEATPalomarTricam
from .neat_maui_geodss import NEATMauiGEODSS
from .skymapper import SkyMapper


class CatchQuery(Base):
    __tablename__ = 'catch_query'
    query_id = Column(Integer, primary_key=True)
    job_id = Column(String(32), index=True,
                    doc="Unique job ID, UUID version 4")
    query = Column(String(128), index=True, doc="User's query string")
    source = Column(String(128), doc="Survey source table queried")
    date = Column(String(64), doc="Date query was executed")
    status = Column(String(64), doc="Query status")
    uncertainty_ellipse = Column(
        Boolean, doc="Query uncertainty_ellipse parameter")
    padding = Column(Float(16), doc="Query padding parameter")


class Caught(Base):
    """Links Found to CatchQuery.

    The combination of query_id and found_id is unique, therefore we use a
    primary key based on the two columns.

    """

    __tablename__ = 'caught'

    query_id = Column(Integer,
                      ForeignKey('catch_query.query_id',
                                 onupdate='CASCADE',
                                 ondelete='CASCADE'),
                      primary_key=True)

    found_id = Column(BigIntegerType,
                      ForeignKey('found.found_id',
                                 onupdate='CASCADE',
                                 ondelete='CASCADE'),
                      primary_key=True)
