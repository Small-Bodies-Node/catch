# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = [
    'NEATMauiGEODSS'
]

import sqlalchemy as sa
from sqlalchemy import Column, String, ForeignKey, Index
from sbsearch.model.core import Base, Observation, BigIntegerType


class NEATMauiGEODSS(Observation):
    __tablename__ = 'neat_maui_geodss'
    __data_source_name__ = 'NEAT Maui GEODSS'
    __obscode__ = '566'

    id = Column(BigIntegerType, primary_key=True)
    observation_id = Column(BigIntegerType,
                            ForeignKey('observation.observation_id',
                                       onupdate='CASCADE',
                                       ondelete='CASCADE'),
                            nullable=False, index=True)

    terms = sa.orm.relationship("NEATMauiGEODSSSpatialTerm",
                                back_populates=__tablename__)

    __mapper_args__ = {
        'polymorphic_identity': 'neat_maui_geodss'
    }

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


class NEATMauiGEODSSSpatialTerm(Base):
    __tablename__ = 'neat_maui_geodss_spatial_terms'
    term_id = Column(BigIntegerType, primary_key=True)
    source_id = Column(BigIntegerType,
                       ForeignKey('neat_maui_geodss.id',
                                  onupdate='CASCADE',
                                  ondelete='CASCADE'),
                       nullable=False, index=True)
    term = Column(String(32), nullable=False)

    neat_maui_geodss = sa.orm.relationship("NEATMauiGEODSS",
                                           back_populates="terms")

    def __repr__(self) -> str:
        return (f'<{self.__class__.__name__} term_id={self.term_id}'
                f' observation_id={self.source_id},'
                f' term={repr(self.term)}>')


NEATMauiGEODSSSpatialTermIndex = Index(
    "ix_neat_maui_geodss_spatial_terms",
    NEATMauiGEODSSSpatialTerm.term
)
