# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""neat_palomar_tricam

To create a new survey, use this file or `sbsearch/model/example_survey.py` from
the sbsearch source code as examples.  The latter has detailed comments on what
to edit.  Then, add the survey to the top of `__init__.py` so that it is
imported and included in `__all__`.

The catch survey data model additionally requires:

* __field_prefix__ - a string used to prefix survey-specific columns in
  aggregated (i.e., multi-survey) output.  Verify that the string length can be
  stored by this column data type.

* product_id - a unique ID for this survey, typically one used at the data
  archive, e.g., a PDS4 logical identifier.

* property: archive_url - returns the URL for the full-sized archive image, or
  else `None`.

* method: cutout_url - returns a URL to retrieve a FITS formatted cutout around
  the requested sky coordinates, or else `None`.

* method: preview_url - same as `cutout_url` except that the URL is for an image
  formatted for a web browser (e.g., JPEG or PNG).

"""

__all__ = ["NEATPalomarTricam"]

from urllib.parse import urlencode
from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation


class NEATPalomarTricam(Observation):
    __tablename__: str = "neat_palomar_tricam"
    __data_source_name__: str = "NEAT Palomar Tricam"
    __obscode__: str = "644"  # MPC observatory code
    __field_prefix__: str = "neat"

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
        String(64), doc="Archive product id", unique=True, index=True, nullable=False
    )

    __mapper_args__ = {"polymorphic_identity": "neat_palomar_tricam"}

    @property
    def archive_url(self):
        """URL to original data product.

        Currently using SBN Comet Sub-node local copy.

        For example:
            https://sbnsurveys.astro.umd.edu/api/images/urn:nasa:pds:gbo.ast.neat.survey:data_tricam:p20020222_obsdata_20020222120052c

        """

        url = f"https://sbnsurveys.astro.umd.edu/api/images/urn:nasa:pds:gbo.ast.neat.survey:data_tricam:{str(self.product_id).lower()}"

        return url

    def cutout_url(self, ra, dec, size=0.0833, format="fits"):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        Currently using SBN Comet Sub-node local copy.

        For example:
            https://sbnsurveys.astro.umd.edu/api/images/urn:nasa:pds:gbo.ast.neat.survey:data_tricam:p20020222_obsdata_20020222120052c?format=jpeg&ra=174.62244&dec=17.97594&size=5arcmin&download=false

        format = fits, jpeg, png

        """

        query_string = urlencode(
            {
                "format": str(format),
                "size": float(size),
                "ra": float(ra),
                "dec": float(dec),
            }
        )

        return f"{self.archive_url}?{query_string}"

    def preview_url(self, ra, dec, size=0.0833, format="jpeg"):
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)
