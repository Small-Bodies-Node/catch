# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['NEATPalomar']

from sqlalchemy import (
    Column, ForeignKey,
    Integer, BigInteger, SmallInteger,
    Float,
    String
)
from sbsearch.schema import Base, Obs, Found, Obj


Obs.__obscode__ = '500'


class NEATPalomar(Obs):
    __tablename__ = 'neat_palomar'
    id = Column(BigInteger, primary_key=True)
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'), index=True)
    productid = Column(String(64), doc='Archive product ID',
                       unique=True, index=True)
    instrument = Column(String(64), doc='Instrument / detector name')
    __mapper_args__ = {
        'polymorphic_identity': 'neat_palomar'
    }
    __data_source_name__ = 'NEAT Palomar'
    __obscode__ = '644'
    __product_path__ = 'neat/tricam/data'


class NEATMauiGEODSS(Obs):
    __tablename__ = 'neat_maui_geodss'
    id = Column(BigInteger, primary_key=True)
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'), index=True)
    productid = Column(String(64), doc='Archive product ID',
                       unique=True, index=True)
    instrument = Column(String(64), doc='Instrument / detector name')
    __mapper_args__ = {
        'polymorphic_identity': 'neat_maui_geodss'
    }
    __data_source_name__ = 'NEAT GEODSS'
    __obscode__ = '566'
    __product_path__ = 'neat/geodss/data'


class PS1DR2(Obs):
    __tablename__ = 'ps1dr2'
    id = Column(BigInteger, primary_key=True, doc='PS1 forcedWarpID')
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'),
                   index=True, nullable=False)
    productid = Column(String(64), doc='Archive product ID',
                       unique=True, index=True, nullable=False)
    telescope_id = Column(SmallInteger, doc='PS1 telescope ID', nullable=False)
    frame_id = Column(Integer, doc='PS1 frame ID', nullable=False)
    projection_id = Column(
        SmallInteger, doc='PS1 projection cell ID', nullable=False)
    skycell_id = Column(SmallInteger, doc='PS1 sky cell ID', nullable=False)
    filter_id = Column(
        SmallInteger, doc='PS1 filter ID: grizy = 1-5', nullable=False)
    __mapper_args__ = {
        'polymorphic_identity': 'ps1dr2'
    }
    __data_source_name__ = 'PanSTARRS 1 DR2'
    __obscode__ = 'F51'
    __product_path__ = None


class SkyMapper(Obs):
    __tablename__ = 'skymapper'
    id = Column(BigInteger, primary_key=True)
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'), index=True)
    productid = Column(String(64), doc='Archive product ID',
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
    __product_path__ = None


class CatchQueries(Base):
    __tablename__ = 'catch_queries'
    queryid = Column(BigInteger, primary_key=True)
    jobid = Column(String(32), index=True,
                   doc="Unique job ID, UUID version 4")
    query = Column(String(128), index=True, doc="User's query string")
    source = Column(String(128), doc="Survey source table queried")
    date = Column(String(64), doc="Date query was executed")
    status = Column(String(64), doc="query status")


class Caught(Base):
    __tablename__ = 'caught'
    queryid = Column(BigInteger, ForeignKey('catch_queries.queryid',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'),
                     primary_key=True)
    foundid = Column(BigInteger, ForeignKey('found.foundid',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'),
                     primary_key=True)
