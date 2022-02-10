# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""catalina

The catch survey data model requires one property and two methods for each
survey object:

* property: archive_url - returns the URL for the full-sized archive image, or
  else `None`.

* method: cutout_url - returns a URL to retrieve a FITS formatted cutout around
  the requested sky coordinates, or else `None`.

* method: preview_url - same as `cutout_url` except that the URL is for an image
  formatted for a web browser (e.g., JPEG or PNG).

"""

__all__ = ["CatalinaBigelow", "CatalinaLemmon", "CatalinaKittPeak"]

from typing import List, Dict
from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation


_ARCHIVE_URL_PREFIX: str = "https://sbnarchive.psi.edu/pds4/surveys/"

_month_to_Mon: Dict[str, str] = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}


class CatalinaBigelow(Observation):
    __tablename__ = "catalina_bigelow"
    __data_source_name__ = "Catalina Sky Survey, Mt. Bigelow"
    __obscode__ = "703"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_bigelow"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {
        "703": "Catalina Sky Survey, 0.7-m Schmidt",
        "V06": "61-inch Kuiper telescope",
    }

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

    @property
    def telescope(self) -> str:
        return self._telescopes.get(self.product_id.split(":")[5][:3])

    @property
    def archive_url(self) -> str:
        # generate from PDS4 LID, e.g.,
        # urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:703_20220120_2b_n02006_01_0001.arch
        # https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated/703/2022/22Jan20/703_20220120_2B_N02006_01_0001.arch.xml
        lid: List[str] = self.product_id.split(":")
        tel: str = lid[5][:3]
        year: str = lid[5][5:9]
        Mon: str = _month_to_Mon[lid[5][9:11]]
        day: str = lid[5][11:13]
        return f"{_ARCHIVE_URL_PREFIX}/{tel}/{year}/{year[-2:]}{Mon}{day}/{lid[5]}.fz"

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


class CatalinaLemmon(Observation):
    __tablename__ = "catalina_lemmon"
    __data_source_name__ = "Catalina Sky Survey, Mt. Lemmon"
    __obscode__ = "G96"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_lemmon"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {
        "G96": "Mount Lemmon Survey, 60-inch telescope",
        "I52": "Mount Lemmon 40-inch follow-up telescope",
    }

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

    @property
    def telescope(self) -> str:
        return self._telescopes.get(self.product_id.split(":")[5][:3])

    @property
    def archive_url(self) -> str:
        # generate from PDS4 LID, e.g.,
        # urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:703_20220120_2b_n02006_01_0001.arch
        # https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated/703/2022/22Jan20/703_20220120_2B_N02006_01_0001.arch.xml
        lid: List[str] = self.product_id.split(":")
        tel: str = lid[5][:3]
        year: str = lid[5][5:9]
        Mon: str = _month_to_Mon[lid[5][9:11]]
        day: str = lid[5][11:13]
        return f"{_ARCHIVE_URL_PREFIX}/{tel}/{year}/{year[-2:]}{Mon}{day}/{lid[5]}.fz"

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


class CatalinaKittPeak(Observation):
    __tablename__ = "catalina_kittpeak"
    __data_source_name__ = "Catalina Sky Survey, Kitt Peak"
    __obscode__ = "V00"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_kittpeak"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {"V00": "Steward Observatory 90-inch Bok telescope"}

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

    @property
    def telescope(self) -> str:
        return self._telescopes.get(self.product_id.split(":")[5][:3])

    @property
    def archive_url(self) -> str:
        # generate from PDS4 LID, e.g.,
        # urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:703_20220120_2b_n02006_01_0001.arch
        # https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated/703/2022/22Jan20/703_20220120_2B_N02006_01_0001.arch.xml
        lid: List[str] = self.product_id.split(":")
        tel: str = lid[5][:3]
        year: str = lid[5][5:9]
        Mon: str = _month_to_Mon[lid[5][9:11]]
        day: str = lid[5][11:13]
        return f"{_ARCHIVE_URL_PREFIX}/{tel}/{year}/{year[-2:]}{Mon}{day}/{lid[5]}.fz"

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/api/get/<product_id>

        format = fits, jpeg, png

        """

        return None

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)
