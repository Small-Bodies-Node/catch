# Licensed with the 3-clause BSD license.  See LICENSE for details.

from typing import List
from sqlalchemy import BigInteger, Column, Integer, String, Float, ForeignKey
from sbsearch.model.core import Base, Observation

__all__: List[str] = ["SkyMapperDR4"]


class SkyMapperDR4(Observation):
    """SkyMapper Souther Sky Survey, data release 4.

    Q55 149.061420.855643-0.516191SkyMapper, Siding Spring

    """

    __tablename__ = "skymapper_dr4"
    __data_source_name__ = "SkyMapperDR4"
    __obscode__ = "413"
    __field_prefix__ = "skymapper"
    __night_offset__ = 0.41

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

    sb_mag = Column(Float(16), doc="Surface brightness estimate (ABmag)")
    field_id = Column(Integer, doc="Field ID", nullable=True)
    image_type = Column(
        String(3),
        doc="Type of image: fs=Shallow Survey, ms=Main Survey, std=Standard Field (images)",
    )
    zpapprox = Column(
        Float(16), doc="Approximate photometric zeropoint for the exposure"
    )

    __mapper_args__ = {"polymorphic_identity": "skymapper_dr4"}

    @property
    def archive_url(self):
        return None

    def cutout_url(self, ra, dec, size=0.0833, format="fits"):
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        https://skymapper.anu.edu.au/how-to-access/#public_siap

        For example:
            https://api.skymapper.nci.org.au/public/siap/dr4/get_image?IMAGE=20140425124821-10&SIZE=0.0833&POS=189.99763,-11.62305&FORMAT=fits

        format = fits, png, or mask

        """

        return (
            "https://api.skymapper.nci.org.au/public/siap/dr4/get_image?"
            f"IMAGE={self.product_id}&SIZE={size}&POS={ra},{dec}&FORMAT={format}"
        )

    def preview_url(self, ra, dec, size=0.0833):
        """Web preview image for cutout."""
        return self.cutout_url(ra, dec, size=size, format="png")
