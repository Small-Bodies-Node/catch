"""Harvest metadata from a local copy of the LONEOS review dataset.

The archive has two collections:

    * urn:nasa:pds:gbo.ast.loneos.survey:data_original
    * urn:nasa:pds:gbo.ast.loneos.survey:data_augmented

Only data_augmented has the field corners.

Files are found in directories named by the LOIS software version used to create
them.  For example:

    * gbo.ast.loneos.survey/data_augmented/lois_3_2_0_beta/041226/041226_2a_082.xml
    * gbo.ast.loneos.survey/data_augmented/lois_4_2_0/051113/051113_1a_011.xml

The path cannot be directly built from the LID:

    * urn:nasa:pds:gbo.ast.loneos.survey:data_augmented:041226_2a_082_fits
    * urn:nasa:pds:gbo.ast.loneos.survey:data_augmented:051113_1a_011_fits

However, the date can be used to determine the directory:

    * Last lois_3_2_0_beta data is 041226
    * First lois 4_2_0 data is 051113

WCS can be off by ~2 arcmin, and there are sky discontinuities in the data.

"""

import os
import logging
import argparse

import numpy as np
from astropy.time import Time
from astropy.coordinates import SkyCoord
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.loneos import LONEOS
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from requests import __version__ as requests_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


# URL prefix for the LONEOS archive at PSI
ARCHIVE_PREFIX = "https://sbnarchive.psi.edu/pds4/surveys/"


class CornerOrderTestFail(Exception):
    pass


class NotLONEOSSkyData(Exception):
    pass


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add LONEOS data to CATCH.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "source",
        help="the root directory of the data collection",
    )
    parser.add_argument(
        "--config",
        default="catch.config",
        type=Config.from_file,
        help="CATCH configuration file",
    )
    parser.add_argument("--log", default="add-loneos.log", help="log file")
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="inspect for new files, but do not add to the database",
    )
    parser.add_argument("-v", action="store_true", help="verbose logging")
    return parser.parse_args()


def process(path):
    # url = "".join((ARCHIVE_PREFIX, path))
    label = pds4_read(path, lazy_load=True, quiet=True).label
    lid = label.find("Identification_Area/logical_identifier").text

    if lid.split(":")[:-1] != [
        "urn",
        "nasa",
        "pds",
        "gbo.ast.loneos.survey",
        "data_augmented",
    ]:
        raise NotLONEOSSkyData(path)

    target_name = label.find(".//Target_Identification/name").text
    if target_name != "Multiple Asteroids":
        raise NotLONEOSSkyData(path)

    obs = LONEOS()

    obs.product_id = lid
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd
    obs.exposure = float(
        label.find(".//img:Imaging/img:Exposure/img:exposure_duration").text
    )

    survey = label.find(".//survey:Survey")
    ra, dec = [], []
    for corner in ("Top Left", "Top Right", "Bottom Right", "Bottom Left"):
        coordinate = survey.find(
            "survey:Image_Corners"
            f"/survey:Corner_Position[survey:corner_identification='{corner}']"
            "/survey:Coordinate"
        )
        ra.append(float(coordinate.find("survey:right_ascension").text))
        dec.append(float(coordinate.find("survey:declination").text))
    obs.set_fov(ra, dec)

    # verify corner order
    coords1 = SkyCoord(ra, dec, unit="deg")
    coords2 = SkyCoord(np.roll(ra, 1), np.roll(dec, 1), unit="deg")
    c = coords1.cartesian.cross(coords2.cartesian)
    test = np.sqrt(np.sum(c.xyz.sum(1) ** 2))
    # expecting a value ~0.001, if it is much smaller then there is an issue
    if test < 1e-4:
        raise CornerOrderTestFail(path)

    return obs


def main():
    args: argparse.Namespace = _parse_args()

    logger = logging.getLogger("add-loneos")
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler(args.log))
    formatter = logging.Formatter("%(levelname)s %(asctime)s (%(name)s): %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.info("Initialized.")
    logger.debug(f"astropy {astropy_version}")
    logger.debug(f"catch {catch_version}")
    logger.debug(f"pds4_tools {pds4_tools_version}")
    logger.debug(f"requests {requests_version}")
    logger.debug(f"sbpy {sbpy_version}")
    logger.debug(f"sbsearch {sbsearch_version}")

    with Catch.with_config(args.config) as catch:
        observations = []
        failed = 0

        tri = ProgressTriangle(1, logger=logger, base=2)
        for dirpath, dirnames, filenames in os.walk(args.source):
            for fn in [f for f in filenames if f.endswith(".xml")]:
                path = os.path.join(dirpath, fn)
                try:
                    observations.append(process(path))
                except NotLONEOSSkyData as e:
                    logger.error("Not LONEOS sky data (%s)", str(e))
                    failed += 1
                    continue
                except CornerOrderTestFail as e:
                    logger.error("Failed corder order test (%s)", str(e))
                    failed += 1
                    continue

                logger.debug("Added: %s", path)
                tri.update()

                if args.dry_run:
                    continue

                if len(observations) >= 10000:
                    catch.add_observations(observations)
                    observations = []

        # add any remaining files
        if not args.dry_run and (len(observations) > 0):
            catch.add_observations(observations)

        if failed > 0:
            logger.warning("Failed processing %d files", failed)

        if not args.dry_run:
            logger.info("Updating survey statistics.")
            catch.update_statistics(source="loneos")


if __name__ == "__main__":
    main()
