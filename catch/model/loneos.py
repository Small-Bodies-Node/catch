# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""LONEOS

To create a new survey, use this file or `sbsearch/model/example_survey.py` from
the sbsearch source code as examples.  The latter has detailed comments on what
to edit.  Then, add the survey to the top of `__init__.py` so that it is
imported and included in `__all__`.

The catch survey data model additionally requires:

* method: cutout_url - returns a URL to retrieve a FITS formatted cutout around
  the requested sky coordinates, or else `None`.

* method: preview_url - same as `cutout_url` except that the URL is for an image
  formatted for a web browser (e.g., JPEG or PNG).

"""

__all__ = ["LONEOS"]

from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation


_ARCHIVE_URL_PREFIX: str = (
    "https://sbnarchive.psi.edu/pds4/surveys"
)


class LONEOS(Observation):
    __tablename__: str = "loneos"
    __data_source_name__: str = "LONEOS"
    __obscode__: str = "699"  # MPC observatory code
    __field_prefix__: str = "loneos"

    source_id = Column(BigInteger, primary_key=True)
    observation_id = Column(
        BigInteger,
        ForeignKey(
            "observation.observation_id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=False,
        index=True,
    )
    product_id = Column(
        String(128), doc="Archive product id", unique=True, index=True, nullable=False
    )

    __mapper_args__ = {"polymorphic_identity": "loneos"}

    @property
    def archive_url(self):
        return None

    def cutout_url(self, ra, dec, size=0.0833, format="fits"):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(self, ra, dec, size=0.0833, format="jpeg"):
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)
