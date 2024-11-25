# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ["NEATMauiGEODSS"]

from urllib.parse import urlencode, quote
from sqlalchemy import BigInteger, Column, String, ForeignKey
from sbsearch.model.core import Observation


_ARCHIVE_URL_PREFIX: str = (
    "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.neat.survey/data_geodss"
)

_CUTOUT_URL_PREFIX: str = "https://sbnsurveys.astro.umd.edu/api/images"


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
        String(100), doc="Archive product id", unique=True, index=True, nullable=False
    )

    __mapper_args__ = {"polymorphic_identity": "neat_maui_geodss"}

    @property
    def archive_url(self) -> str:
        """URL to original data product.

        urn:nasa:pds:gbo.ast.neat.survey:data_geodss:g19960514_obsdata_960514061638d

        https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.neat.survey/data_geodss/g19960514/
        obsdata/960514061638d.fit.fz

        """

        # url = "https://sbnsurveys.astro.umd.edu/api/images/" + quote(
        #    f"urn:nasa:pds:gbo.ast.neat.survey:data_geodss:{str(self.product_id).lower()}"
        # )

        product_id = self.product_id[self.product_id.rindex(":") + 1 :]
        path = product_id.replace("_", "/")

        url = f"{_ARCHIVE_URL_PREFIX}/{path}.fit.fz"

        return url

    @property
    def label_url(self) -> str:
        """URL to PDS4 label.

        urn:nasa:pds:gbo.ast.neat.survey:data_geodss:g19960514_obsdata_960514061638d

        https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.neat.survey/data_geodss/g19960514/
        obsdata/960514061638d.xml

        """

        return self.archive_url[:-6] + "xml"

    def cutout_url(self, ra, dec, size=0.12, format="fits") -> str:
        """URL to cutout ``size`` around ``ra``, ``dec`` in deg.

        Currently via SBN Comet Sub-node.

        For example:
            https://sbnsurveys.astro.umd.edu/api/images/urn:nasa:pds:gbo.ast.neat.survey:
            data_geodss:g19960621_obsdata_960621121818a?format=jpeg&ra=286.8054&dec=-12.8289
            &size=5arcmin&download=false

        format = fits, jpeg, png

        """

        query_string = urlencode(
            {
                "ra": float(ra),
                "dec": float(dec),
                "size": "{:.2f}arcmin".format(float(size) * 60),
                "format": str(format),
            }
        )

        return f"{_CUTOUT_URL_PREFIX}/{self.product_id}?{query_string}"

    def preview_url(self, ra, dec, size=0.12, format="jpeg"):
        """Web preview image for cutout."""
        return self.cutout_url(ra, dec, size=size, format=format)
