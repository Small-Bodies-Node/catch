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


class Caught(Base):
    __tablename__ = 'caught'
    caughtid = Column(BigInteger, primary_key=True)
    sessionid = Column(String(64), index=True)
    query = Column(String(128), doc="User's query string")
    obsid = Column(BigInteger, ForeignKey('obs.obsid', onupdate='CASCADE',
                                          ondelete='CASCADE'))
    desg = Column(String(64), doc='Target designation')
    ra = Column(Float(32), doc='Right Ascension ICRF, deg')
    dec = Column(Float(32), doc='Declination ICRF, deg')
    dra = Column(Float(32), doc='arcsec/hr')
    dra = Column(Float(32), doc=('sky motion, includes cos(Dec) factor,'
                                 ' arcsec/hr'))
    ddec = Column(Float(32), doc='arcsec/hr')
    unc_a = Column(Float(32), doc='error ellipse semi-major axis, arcsec')
    unc_b = Column(Float(32), doc='error ellipse semi-minor axis, arcsec')
    unc_theta = Column(Float(32), doc='error ellipse position angle, deg')
    vmag = Column(Float(32), doc='predicted visual brightness, mag')
    rh = Column(Float(32), doc='heliocentric distance, au')
    rdot = Column(Float(32), doc='heliocentric radial velocity, km/s')
    delta = Column(Float(32), doc='observer-target distance, au')
    phase = Column(Float(32), doc='Sun-observer-target angle, deg')
    selong = Column(Float(32), doc='solar elongation, deg')
    sangle = Column(Float(32), doc=('projected target-Sun vector position'
                                    ' angle, deg'))
    vangle = Column(Float(32), doc=('projected target velocity vector'
                                    ' position angle, deg'))
    trueanomaly = Column(Float(32), doc='deg')
    tmtp = Column(Float(32), doc='time from perihelion, T-T_P, days')
