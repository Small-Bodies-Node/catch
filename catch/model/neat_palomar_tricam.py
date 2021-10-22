# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""neat_palomar_tricam

To create a new survey, use this file or `sbsearch/model/example_survey.py` from
the sbsearch source code as examples.  The latter has detailed comments on what
to edit.

The catch survey data model requires one property and two methods for each
survey object:

* property: archive_url - returns the URL for the full-sized archive image, or
  else `None`.

* method: cutout_url - returns a URL to retrieve a FITS formatted cutout around
  the requested sky coordinates, or else `None`.

* method: preview_url - same as `cutout_url` except that the URL is for an image
  formatted for a web browser (e.g., JPEG or PNG).

"""

__all__ = [
    'NEATPalomarTricam'
]

import sqlalchemy as sa
from sqlalchemy import Column, String, ForeignKey, Index
from sbsearch.model.core import Base, Observation, BigIntegerType


class NEATPalomarTricam(Observation):
    __tablename__ = 'neat_palomar_tricam'
    __data_source_name__ = 'NEAT Palomar Tricam'
    __obscode__ = '644'  # MPC observatory code

    id = Column(BigIntegerType, primary_key=True)
    observation_id = Column(BigIntegerType,
                            ForeignKey('observation.observation_id',
                                       onupdate='CASCADE',
                                       ondelete='CASCADE'),
                            nullable=False, index=True)

    terms = sa.orm.relationship("NEATPalomarTricamSpatialTerm",
                                back_populates=__tablename__)

    __mapper_args__ = {
        'polymorphic_identity': 'neat_palomar_tricam'
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
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


class NEATPalomarTricamSpatialTerm(Base):
    __tablename__ = 'neat_palomar_tricam_spatial_terms'
    term_id = Column(BigIntegerType, primary_key=True)
    source_id = Column(BigIntegerType,
                       ForeignKey('neat_palomar_tricam.id',
                                  onupdate='CASCADE',
                                  ondelete='CASCADE'),
                       nullable=False, index=True)
    term = Column(String(32), nullable=False)

    neat_palomar_tricam = sa.orm.relationship("NEATPalomarTricam",
                                              back_populates="terms")

    def __repr__(self) -> str:
        return (f'<{self.__class__.__name__} term_id={self.term_id}'
                f' observation_id={self.source_id},'
                f' term={repr(self.term)}>')


NEATPalomarTricamSpatialTermIndex = Index(
    "ix_neat_palomar_tricam_spatial_terms",
    NEATPalomarTricamSpatialTerm.term
)
