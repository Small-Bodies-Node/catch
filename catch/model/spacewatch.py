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
    "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data"
)

_CUTOUT_URL_PREFIX: str = (
    "https://uxzqjwo0ye.execute-api.us-west-1.amazonaws.com/api/images"
)


class Spacewatch(Observation):
    """Spacewatch

    691 248.399660.849466+0.526479Steward Observatory, Kitt Peak-Spacewatch

    """

    __tablename__: str = "spacewatch"
    __data_source_name__: str = "Spacewatch"
    __obscode__: str = "691"  # MPC observatory code
    __field_prefix__: str = "spacewatch"
    __night_offset__: float = -0.31

    source_id = Column(BigInteger, primary_key=True)
    observation_id = Column(
        BigInteger,
        ForeignKey(
            "observation.observation_id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=False,
        index=True,
    )
    product_id: str = Column(
        String(128), doc="Archive product id", unique=True, index=True, nullable=False
    )
    # file name is needed, I don't see a way to guess file names from every LID:
    # the case is important for some files
    file_name: str = Column(
        String(64), doc="Archive data product file name", nullable=False
    )

    __mapper_args__ = {"polymorphic_identity": "spacewatch"}

    @property
    def label_url(self):
        return self.archive_url[:-4] + "xml"

    @property
    def archive_url(self):
        product_id = self.product_id[self.product_id.rindex(":") + 1 :]
        y, m, d = product_id.split("_")[-6:-3]
        return f"{_ARCHIVE_URL_PREFIX}/{y}/{m}/{d}/{self.file_name}"

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:

        format = fits, jpeg, png

        """

        size_arcmin: float = max(0.01, size * 60)

        # fix the case to match the file_name, at bit of a hack, but will work
        # until we have a better image service deployed

        lid = self.product_id
        cased_lid = lid[: -len(self.file_name)] + self.file_name
        return (
            f"{_CUTOUT_URL_PREFIX}/{cased_lid}"
            f"?ra={ra}&dec={dec}&size={size_arcmin:.2f}arcmin"
            f"&format={format}"
        )

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)
