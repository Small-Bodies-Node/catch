# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""spacewatch

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

__all__ = ["Spacewatch"]

from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation


_ARCHIVE_URL_PREFIX: str = (
    "https://sbnarchive.psi.edu/pds4/surveys"
)


class Spacewatch(Observation):
    __tablename__: str = "spacewatch"
    __data_source_name__: str = "Spacewatch"
    __obscode__: str = "691"  # MPC observatory code
    __field_prefix__: str = "spacewatch"

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
    label = Column(
        String(128), doc="PDS4 label file name and path.",
    )

    __mapper_args__ = {"polymorphic_identity": "spacewatch"}

    @property
    def archive_url(self):
        return "/".join((_ARCHIVE_URL_PREFIX, self.label.replace('.xml', '.fits')))

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
