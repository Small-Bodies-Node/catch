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

    @property
    def archive_url(self):
        return None

    def cutout_url(self, ra, dec, size=0.0833, format='fits'):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(self, ra, dec, size=0.0833, format='jpeg'):
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


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

    @property
    def archive_url(self):
        return None

    def cutout_url(self, ra, dec, size=0.0833, format='fits'):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(self, ra, dec, size=0.0833, format='jpeg'):
        """Web preview image for cutout."""
        return self.cutout_url(ra, dec, size=size, format=format)


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
        3), doc='Type of image: fs=Fast Survey, ms=Main Survey, std=Standard Field (images)')
    zpapprox = Column(
        Float(16), doc='Approximate photometric zeropoint for the exposure')
    __mapper_args__ = {
        'polymorphic_identity': 'skymapper'
    }
    __data_source_name__ = 'SkyMapper'
    __obscode__ = '413'

    @property
    def archive_url(self):
        return None

    def cutout_url(self, ra, dec, size=0.0833, format='fits'):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        https://skymapper.anu.edu.au/how-to-access/#public_siap

        For example:
            https://api.skymapper.nci.org.au/public/siap/dr2/get_image?IMAGE=20140425124821-10&SIZE=0.0833&POS=189.99763,-11.62305&FORMAT=fits

        format = fits, png, or mask

        """

        return (
            'https://api.skymapper.nci.org.au/public/siap/dr2/get_image?'
            f'IMAGE={self.product_id}&SIZE={size}&POS={ra},{dec}&FORMAT={format}'
        )

    def preview_url(self, ra, dec, size=0.0833):
        """Web preview image for cutout."""
        return self.cutout_url(ra, dec, size=size, format='png')


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
