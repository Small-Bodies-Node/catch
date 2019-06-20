# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['NEATPalomar']

from sqlalchemy import Column, Integer, BigInteger, Float, String, ForeignKey
from sbsearch.schema import Base, Obs


class NEATPalomar(Obs):
    __tablename__ = 'neat_palomar'
    id = Column(BigInteger, primary_key=True)
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'))
    ra_c = Column(Float(32), doc='Right Ascension of field center (deg)')
    dec_c = Column(Float(32), doc='Declination of field center (deg)')
    productid = Column(String(64), doc='Archive product id', unique=True)
    instrument = Column(String(64), doc='Instrument / detector name')
    __mapper_args__ = {
        'polymorphic_identity': 'neat_palomar'
    }


class CatchQueries(Base):
    __tablename__ = 'catch_queries'
    queryid = Column(BigInteger, primary_key=True)
    sessionid = Column(String(64), doc="User's unique session ID")
    query = Column(String(128), doc="User's query string")


class Caught(Base):
    __tablename__ = 'caught'
    caughtid = Column(BigInteger, primary_key=True)
    queryid = Column(BigInteger, ForeignKey(
        'catch_queries.queryid', onupdate='CASCADE', ondelete='CASCADE'))
    obsid = Column(BigInteger, ForeignKey(
        'obs.obsid', onupdate='CASCADE', ondelete='CASCADE'))
    foundid = Column(BigInteger, ForeignKey(
        'found.foundid', onupdate='CASCADE', ondelete='CASCADE'))
