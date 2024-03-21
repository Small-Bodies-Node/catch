# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""catalina

Catalina Sky Survey archive at PDS

"""

__all__ = ["CatalinaBigelow", "CatalinaLemmon", "CatalinaBokNEOSurvey"]

from typing import List, Dict
from urllib.parse import quote
from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Base, Observation

_ARCHIVE_URL_PREFIX: str = (
    "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated"
)

_CUTOUT_URL_PREFIX: str = (
    "https://5ub5yo2kmj.execute-api.us-east-1.amazonaws.com/api/images"
)

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


class CatalinaSkySurvey:
    """CSS specific functions / properties."""

    __field_prefix__: str = "css"

    @property
    def telescope(self) -> str:
        tel: str = self.product_id.split(":")[5][:3].upper()
        return self._telescopes.get(tel)

    @property
    def archive_url(self) -> str:
        # generate from PDS4 LID, e.g.,
        # urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:703_20220120_2b_n02006_01_0001.arch
        # https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated/703/2022/22Jan20/703_20220120_2B_N02006_01_0001.arch.xml
        lid: List[str] = self.product_id.split(":")
        tel: str
        date: str
        tel, date = lid[5].split("_")[:2]
        year: str = date[:4]
        Mon: str = _month_to_Mon[date[4:6]]
        day: str = date[6:]
        i: int = lid[5].find(".")
        prefix: str = lid[5][:i].upper()
        suffix: str = lid[5][i:].lower() + ".fz"
        return f"{_ARCHIVE_URL_PREFIX}/{tel.upper()}/{year}/{year[-2:]}{Mon}{day}/{prefix}{suffix}"

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:

        format = fits, jpeg, png

        """

        size_arcmin: float = max(0.01, size * 60)

        return (
            f"{_CUTOUT_URL_PREFIX}/{quote(self.product_id)}"
            f"?ra={ra}&dec={dec}&size={size_arcmin:.2f}arcmin"
            f"&format={format}"
        )

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


class CatalinaBigelow(Observation, CatalinaSkySurvey):
    __tablename__ = "catalina_bigelow"
    __data_source_name__ = "Catalina Sky Survey, Mt. Bigelow"
    __obscode__ = "703"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_bigelow"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {
        "703": "Catalina Sky Survey, 0.7-m Schmidt (703)",
        "V06": "61-inch Kuiper telescope (V06)",
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


class CatalinaLemmon(Observation, CatalinaSkySurvey):
    __tablename__ = "catalina_lemmon"
    __data_source_name__ = "Catalina Sky Survey, Mt. Lemmon"
    __obscode__ = "G96"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_lemmon"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {
        "G96": "Mount Lemmon Survey, 60-inch telescope (G96)",
        "I52": "Mount Lemmon 40-inch follow-up telescope (I52)",
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


class CatalinaBokNEOSurvey(Observation, CatalinaSkySurvey):
    __tablename__ = "catalina_bokneosurvey"
    __data_source_name__ = "Catalina Sky Survey Archive, Bok NEO Survey"
    __obscode__ = "V00"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "catalina_bokneosurvey"}

    # telescopes included at this site
    # MPC code : name
    _telescopes = {"V00": "Steward Observatory 90-inch Bok telescope (V00)"}

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
