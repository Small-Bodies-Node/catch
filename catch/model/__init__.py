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
    'PS1DR2',
]

from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sbsearch.model.core import Base, Observation, Found, Ephemeris, Obj
from sbsearch.model.example_survey import ExampleSurvey
from sqlalchemy.sql.sqltypes import Boolean
from .neat_palomar_tricam import NEATPalomarTricam
from .neat_maui_geodss import NEATMauiGEODSS
from .skymapper import SkyMapper
from .ps1dr2 import PS1DR2


class CatchQuery(Base):
    __tablename__ = 'catch_query'
    query_id = Column(Integer, primary_key=True)
    job_id = Column(String(32), index=True,
                    doc="Unique job ID, UUID version 4")
    query = Column(String(128), index=True, doc="User's query string")
    source = Column(String(128), doc="Survey source table queried")
    uncertainty_ellipse = Column(
        Boolean, doc="Query uncertainty_ellipse parameter")
    padding = Column(Float(16), doc="Query padding parameter")
    date = Column(String(64), doc="Date query was executed")
    status = Column(String(64), doc="Query status")


# Add CATCH specific columns
Found.query_id = Column(Integer,
                        ForeignKey('catch_query.query_id', onupdate='CASCADE',
                                   ondelete='CASCADE'),
                        index=True)
