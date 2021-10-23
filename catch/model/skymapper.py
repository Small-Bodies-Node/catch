# Licensed with the 3-clause BSD license.  See LICENSE for details.

from typing import List
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index

from sbsearch.model.core import BigIntegerType, Base, Observation

__all__: List[str] = [
    'SkyMapper'
]


class SkyMapper(Observation):
    __tablename__ = 'skymapper'
    __data_source_name__ = 'SkyMapper'
    __obscode__ = '413'

    id = Column(BigIntegerType, primary_key=True)
    observation_id = Column(BigIntegerType,
                            ForeignKey('observation.observation_id',
                                       onupdate='CASCADE',
                                       ondelete='CASCADE'),
                            nullable=False,
                            index=True)

    terms = sa.orm.relationship("SkyMapperSpatialTerm",
                                back_populates='source')

    sb_mag = Column(Float(16), doc='Surface brightness estimate (ABmag)')
    field_id = Column(Integer, doc='Field ID')
    image_type = Column(String(
        3), doc='Type of image: fs=Fast Survey, ms=Main Survey, std=Standard Field (images)')
    zpapprox = Column(
        Float(16), doc='Approximate photometric zeropoint for the exposure')

    __mapper_args__ = {
        'polymorphic_identity': 'skymapper'
    }

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


class SkyMapperSpatialTerm(Base):
    __tablename__ = 'skymapper_spatial_terms'
    term_id = Column(BigIntegerType, primary_key=True)
    source_id = Column(BigIntegerType,
                       ForeignKey('skymapper.id', onupdate='CASCADE',
                                  ondelete='CASCADE'),
                       nullable=False, index=True)
    term = Column(String(32), nullable=False)

    source = sa.orm.relationship("SkyMapper", back_populates="terms")

    def __repr__(self) -> str:
        return (f'<{self.__class__.__name__} term_id={self.term_id}'
                f' observation_id={self.source_id},'
                f' term={repr(self.term)}>')


SkyMapperSpatialTermIndex = Index(
    "ix_skymapper_spatial_terms",
    SkyMapperSpatialTerm.term
)
