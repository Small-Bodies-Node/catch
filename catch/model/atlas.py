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

__all__ = ["ATLASMaunaLoa", "ATLASHaleakela", "ATLASRioHurtado", "ATLASSutherland"]

from typing import Union
from sqlalchemy import BigInteger, Column, String, Boolean, ForeignKey
from sbsearch.model.core import Observation


_ARCHIVE_URL_PREFIX: str = "https://sbnsurveys.astro.umd.edu/images/"


class ATLAS:
    """ATLAS specific functions / properties."""

    __field_prefix__: str = "atlas"

    @property
    def archive_url(self) -> str:
        # generate from PDS4 LID, e.g.,
        # urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
        lid: str = self.product_id
        return f"{_ARCHIVE_URL_PREFIX}/{lid}"

    @property
    def label_url(self) -> str:
        lid: str = self.product_id
        return f"{_ARCHIVE_URL_PREFIX}/{lid}?format=label"

    @property
    def diff_url(self) -> Union[str, None]:
        # generate URL to difference image based on prime LID
        if self.diff:
            lid: str = self.product_id.replace("_fits", "_diff")
            return f"{_ARCHIVE_URL_PREFIX}/{lid}"
        else:
            # this image has no diff
            return None

    def cutout_url(
        self,
        ra: float,
        dec: float,
        size: float = 0.16,
        format: str = "fits",
        diff: bool = False,
    ) -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        For example:
            https://sbnsurveys.astro.umd.edu/images/<product_id>?ra=123&dec=32&size=5arcmin&format=fits

        format = fits, jpeg, png

        """

        if diff and not self.diff:
            return None

        base_url: str = self.diff_url if diff else self.archive_url
        return f"{base_url}/ra={float(ra)}&dec={float(dec)}&size={float(size)}deg&format={format}"

    def diff_cutout_url(self, *args, **kwargs):
        """URL to cutout for difference image."""
        return self.cutout_url(*args, diff=True, **kwargs)

    def diff_preview_url(self, *args, **kwargs):
        """URL to cutout preview for difference image."""
        return self.preview_url(*args, diff=True, **kwargs)

    def preview_url(
        self, ra: float, dec: float, size: float = 0.16, format: str = "jpeg"
    ) -> str:
        """Web preview image."""
        return self.cutout_url(ra, dec, size=size, format=format)


class ATLASMaunaLoa(Observation, ATLAS):
    """ATLAS Mauna Loa

    Note that there are two sites on Mauna Loa:

    T07 204.423870.943290+0.332467ATLAS-MLO Auxiliary Camera, Mauna Loa
    T08 204.423950.943290+0.332467ATLAS-MLO, Mauna Loa

    At present, PDS is only archiving data at T08.

    """

    __tablename__ = "atlas_mauna_loa"
    __data_source_name__ = "ATLAS Hawaii, Mauna Loa"
    __obscode__ = "T08"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "atlas_mauna_loa"}

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
    field_id = Column(String(32), doc="Survey field ID", index=True, nullable=False)
    diff = Column(Boolean, doc="True if a difference image exists", nullable=False)


class ATLASHaleakela(Observation, ATLAS):
    """ATLAS Haleakela"""

    __tablename__ = "atlas_haleakela"
    __data_source_name__ = "ATLAS Hawaii, Haleakela"
    __obscode__ = "T05"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "atlas_haleakela"}

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
    field_id = Column(String(32), doc="Survey field ID", index=True, nullable=False)
    diff = Column(Boolean, doc="True if a difference image exists", nullable=False)


class ATLASRioHurtado(Observation, ATLAS):
    """ATLAS Chile, Rio Hurtado"""

    __tablename__ = "atlas_rio_hurtado"
    __data_source_name__ = "ATLAS Chile, Rio Hurtado"
    __obscode__ = "W68"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "atlas_rio_hurtado"}

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
    field_id = Column(String(32), doc="Survey field ID", index=True, nullable=False)
    diff = Column(Boolean, doc="True if a difference image exists", nullable=False)


class ATLASSutherland(Observation, ATLAS):
    """ATLAS South Africa, Sutherland"""

    __tablename__ = "atlas_sutherland"
    __data_source_name__ = "ATLAS South Africa, Sutherland"
    __obscode__ = "M22"  # MPC observatory code
    __mapper_args__ = {"polymorphic_identity": "atlas_sutherland"}

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
    field_id = Column(String(32), doc="Survey field ID", index=True, nullable=False)
    diff = Column(Boolean, doc="True if a difference image exists", nullable=False)
