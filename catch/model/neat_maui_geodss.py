# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ["NEATMauiGEODSS"]

from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation


class NEATMauiGEODSS(Observation):
    __tablename__ = "neat_maui_geodss"
    __data_source_name__ = "NEAT Maui GEODSS"
    __obscode__ = "566"
    __field_prefix__ = "neat"

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

    __mapper_args__ = {"polymorphic_identity": "neat_maui_geodss"}

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
        """Web preview image for cutout."""
        return self.cutout_url(ra, dec, size=size, format=format)
