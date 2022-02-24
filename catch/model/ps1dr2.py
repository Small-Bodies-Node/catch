# Licensed with the 3-clause BSD license.  See LICENSE for details.
"""ps1dr2

PanSTARRS 1 Data Release 2

"""

__all__ = ["PS1DR2"]

from sqlalchemy import BigInteger, Column, Integer, SmallInteger, String, ForeignKey
from sbsearch.model.core import Base, Observation


class PS1DR2(Observation):
    __tablename__ = "ps1dr2"
    __data_source_name__ = "PanSTARRS 1 DR2"
    __obscode__ = "F51"  # MPC observatory code
    __field_prefix__ = "ps1"

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
    telescope_id = Column(SmallInteger, doc="PS1 telescope ID", nullable=False)
    frame_id = Column(Integer, doc="PS1 frame ID", nullable=False)
    projection_id = Column(SmallInteger, doc="PS1 projection cell ID", nullable=False)
    skycell_id = Column(SmallInteger, doc="PS1 sky cell ID", nullable=False)
    filter_id = Column(SmallInteger, doc="PS1 filter ID: grizy = 1-5", nullable=False)

    __mapper_args__ = {"polymorphic_identity": "ps1dr2"}

    @property
    def _warp_path(self) -> str:
        url: str = (
            f"/rings.v3.skycell/{self.projection_id:04d}/"
            f"{self.skycell_id:03d}/{self.product_id}"
        )
        return url

    @property
    def archive_url(self) -> str:
        """Get URL to this PS1 DR2 skycell warp image.

        For example:
            https://ps1images.stsci.edu/rings.v3.skycell/0798/045/rings.v3.skycell.0798.045.wrp.i.56533_48941.fits

        /rings.v3.skycell/{projectionid:04d}/{skycellid:03d}/rings.v3.skycell.{projectionid:04d}.{skycellid:03d}.wrp.{filter}.{date_sfx}.fits
        date_sfx = mjd[:11].replace('.', '_')
        filter from filterid: 1-5 for grizy (Flewelling et al. 2019)

        """
        url: str = f"https://ps1images.stsci.edu{self._warp_path}"
        return url

    def cutout_url(
        self, ra: float, dec: float, size: float = 0.05, format: str = "fits"
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://ps1images.stsci.edu/cgi-bin/fitscut.cgi?red=/rings.v3.skycell/1405/053/rings.v3.skycell.1405.053.stk.g.unconv.fits&ra=332.4875&dec=2.14639&size=300&format=jpeg

        format = fits, jpeg, png

        """
        pixels: int = int(size * 3600 / 0.25)  # 0.25"/pixel
        url: str = (
            "https://ps1images.stsci.edu/cgi-bin/fitscut.cgi?"
            f"red={self._warp_path}&ra={ra}&dec={dec}"
            f"&size={pixels}&format={format}"
        )
        return url

    def preview_url(
        self, ra: float, dec: float, size: float = 0.0833, format: str = "jpeg"
    ) -> str:
        """Web preview image.  format = jpeg, png"""
        return self.cutout_url(ra, dec, size=size, format=format)
