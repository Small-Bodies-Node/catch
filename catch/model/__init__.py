# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""model

To add surveys, see neat_palomar_tricam.py for instructions.

"""

__all__ = [
    "CatchQuery",
    "SurveyStats",
    "Observation",
    "Ephemeris",
    "Found",
    "Obj",
    "NEATPalomarTricam",
    "NEATMauiGEODSS",
    "SkyMapper",
    "PS1DR2",
    "CatalinaBigelow",
    "CatalinaLemmon",
    "CatalinaBokNEOSurvey",
    "Spacewatch",
    "LONEOS",
]

from sqlalchemy import Column, Integer, Float, String, ForeignKey, Text
from sbsearch.model.core import Base, Observation, Found, Ephemeris, Obj
from sbsearch.model.example_survey import ExampleSurvey
from sqlalchemy.sql.sqltypes import Boolean
from .neat_palomar_tricam import NEATPalomarTricam
from .neat_maui_geodss import NEATMauiGEODSS
from .skymapper import SkyMapper
from .ps1dr2 import PS1DR2
from .catalina import CatalinaBigelow, CatalinaLemmon, CatalinaBokNEOSurvey
from .spacewatch import Spacewatch
from .loneos import LONEOS


# database version
version: str = "2.1"


class CatchQuery(Base):
    __tablename__ = f"catch_query_{version.replace('.', '_')}"
    query_id = Column(
        Integer,
        primary_key=True,
    )
    job_id = Column(
        String(32),
        index=True,
        doc="Unique job ID, UUID version 4",
    )
    query = Column(
        String(128),
        index=True,
        doc="User's query string",
    )
    source = Column(
        String(512),
        doc="Survey source table queried",
    )
    uncertainty_ellipse = Column(
        Boolean,
        doc="Query uncertainty_ellipse parameter",
    )
    padding = Column(
        Float(16),
        doc="Query padding parameter, arcmin",
    )
    intersection_type = Column(
        String(32),
        doc="Fixed target areal search intersection type",
        nullable=True,
    )
    start_date = Column(
        String(32),
        doc="Query range start date",
        nullable=True,
    )
    stop_date = Column(
        String(32),
        doc="Query range stop date",
        nullable=True,
    )
    date = Column(
        String(64),
        doc="Date query was executed",
    )
    execution_time = Column(
        Float(16),
        nullable=True,
        doc="Query execution time (wall clock, seconds), or null for cached results",
    )
    status = Column(String(64), doc="Query status")

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}"
            f" query_id={self.query_id},"
            f" job_id={self.job_id},"
            f" query={self.query}"
            f" source={repr(self.source)}"
            f" uncertainty_ellipse={self.uncertainty_ellipse}"
            f" padding={self.padding}"
            f" intersection='{self.intersection_type}'"
            f" start_date='{self.start_date}'"
            f" stop_date='{self.stop_date}'"
            f" date={self.date}"
            f" execution_time={self.execution_time}"
            f" status={self.status}>"
        )


class SurveyStats(Base):
    __tablename__ = "survey_statistics"
    stats_id = Column(Integer, primary_key=True)
    source = Column(Text, doc="Source survey ID")
    name = Column(Text, doc="Survey name")
    count = Column(Integer, doc="Number of data products")
    start_date = Column(Text, doc="First data product start date, UTC")
    stop_date = Column(Text, doc="Last data prodcut stop date, UTC")
    updated = Column(Text, doc="Date these statistics were updated")


# Version found table and add CATCH specific columns
Found.__tablename__ = f"found_{version.replace('.', '_')}"
Found.query_id = Column(
    Integer,
    ForeignKey(
        f"{CatchQuery.__tablename__}.query_id",
        onupdate="CASCADE",
        ondelete="CASCADE",
    ),
    index=True,
)
