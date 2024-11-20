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
from sbsearch.model.core import Observation


_ARCHIVE_URL_PREFIX: str = "https://sbnarchive.psi.edu/pds4/surveys"

_CUTOUT_URL_PREFIX: str = (
    "https://uxzqjwo0ye.execute-api.us-west-1.amazonaws.com/api/images"
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
        """Augmented data product at PSI.

        urn:nasa:pds:gbo.ast.loneos.survey:data_augmented:041226_2a_082_fits
        -> https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.loneos.survey/data_augmented/
           lois_3_2_0_beta/041226/041226_2a_082.fits

        urn:nasa:pds:gbo.ast.loneos.survey:data_augmented:051113_1a_011_fits
        -> https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.loneos.survey/data_augmented/
           lois_4_2_0/051113/051113_1a_011.fits

        Using the date to determine the URL:
            * Last lois_3_2_0_beta data is 041226
            * First lois 4_2_0 data is 051113

        """

        product_id: str = self.product_id.split(":")[-1]
        fn: str = product_id[:-5] + ".fits"
        date: str = product_id[:6]

        lois: str
        if date < "050101":
            lois = "lois_3_2_0_beta"
        else:
            lois = "lois_4_2_0"

        return f"{_ARCHIVE_URL_PREFIX}/gbo.ast.loneos.survey/data_augmented/{lois}/{date}/{fn}"

    def cutout_url(self, ra, dec, size=0.21, format="fits"):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        format = fits, jpeg, png

        https://uxzqjwo0ye.execute-api.us-west-1.amazonaws.com/api/images/urn:nasa:pds:gbo.ast.loneos.survey:data_augmented:051113_1a_011_fits?ra=320.8154669&dec=9.1222266&size=5arcmin&format=fits

        """

        size_arcmin: float = max(0.01, size * 60)

        return (
            f"{_CUTOUT_URL_PREFIX}/{self.product_id}"
            f"?ra={ra}&dec={dec}&size={size_arcmin:.2f}arcmin"
            f"&format={format}"
        )

    def preview_url(self, ra, dec, size=0.21, format="jpeg"):
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)
