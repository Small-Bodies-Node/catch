"""Harvest ATLAS metadata.

Files are fz compressed.  We just want labels with LIDs ending in _fits.

"""

import os
import argparse
import logging
from glob import iglob

from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.atlas import ATLASMaunaLoa, ATLASHaleakela, ATLASRioHurtado, ATLASSutherland
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


def setup_logger(log_filename):
    logger = logging.getLogger("add-atlas")
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler(log_filename))
    formatter = logging.Formatter(
        "%(levelname)s %(asctime)s (%(name)s): %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.info("Initialized.")
    logger.debug(f"astropy {astropy_version}")
    logger.debug(f"catch {catch_version}")
    logger.debug(f"pds4_tools {pds4_tools_version}")
    logger.debug(f"sbpy {sbpy_version}")
    logger.debug(f"sbsearch {sbsearch_version}")
    return logger


def inventory(base_path):
    """Find ATLAS labels within this path.

    This function was designed for the ATLAS review, July/August 2022.  Please
    update for production, when ready.

    atlas
    ├── data
    │   └── 59613
    └── document

    Labels we want are *.fits.xml

    """

    for dname in iglob(f"{base_path}/atlas/data/5*"):
        if os.path.isdir(dname):
            for fn in iglob(f"{dname}/*.fits.xml"):
                label = pds4_read(fn, lazy_load=True, quiet=True).label
                lid = label.find("Identification_Area/logical_identifier").text
                if lid.endswith("_fits"):
                    # Should be good!
                    yield fn, label


def get_obs_model(lid):
    # example LID: urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
    tel = lid.split(':')[-1][:2]
    return {'01': ATLASMaunaLoa,
            '02': ATLASHaleakela,
            '03': ATLASSutherland,
            '04': ATLASRioHurtado}[tel]


def process(fn, label):
    lid = label.find("Identification_Area/logical_identifier").text
    obs = get_obs_model(lid)()

    obs.product_id = lid
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd
    obs.exposure = float(label.find(
        ".//img:Exposure/img:exposure_duration").text),
    obs.filter = label.find(".//img:Optical_Filter/img:filter_name").text

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

    maglimit = survey.find(".//survey:N_Sigma_Limit/survey:limiting_magnitude")
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    return obs


parser = argparse.ArgumentParser(
    description="Add ATLAS data to CATCH.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument('base_path', help="path to data collection root directory")
parser.add_argument(
    "--config",
    default="catch.config",
    type=Config.from_file,
    help="CATCH configuration file",
)
parser.add_argument("--log", default="add-atlas.log", help="log file")
parser.add_argument("-v", action="store_true", help="verbose logging")
parser.add_argument(
    "--dry-run",
    "-n",
    action="store_true",
    help="process labels, but do not add to the database",
)
parser.add_argument(
    "-t",
    action="store_true",
    help="just test for the existence of the files (implies -n)"
)

args = parser.parse_args()

logger = setup_logger(args.log)

if not os.path.exists(f"{args.base_path}/atlas"):
    raise ValueError(
        f"atlas not found in {args.base_path}")

if args.t:
    logger.info("Testing for existence of all files.")

if args.dry_run or args.t:
    logger.info("Dry run, databases will not be updated.")

if args.v:
    logger.setLevel(logging.DEBUG)

with Catch.with_config(args.config) as catch:
    observations = []
    failed = 0

    tri = ProgressTriangle(1, logger=logger, base=2)
    for fn, label in inventory(args.base_path):
        tri.update()

        if args.t:
            if not os.path.exists(fn):
                logger.error("Missing %s", fn)
            continue

        try:
            observations.append(process(fn, label))
            msg = "added"
        except ValueError as e:
            failed += 1
            msg = str(e)
        except:
            logger.error(
                "A fatal error occurred processing %s", fn, exc_info=True
            )
            raise

        logger.debug("%s: %s", fn, msg)

        if args.dry_run or args.t:
            continue

        if len(observations) >= 8192:
            try:
                catch.add_observations(observations)
            except:
                logger.error("A fatal error occurred saving data to the database.",
                             exc_info=True)
                raise
            observations = []

    # add any remaining files
    if not (args.dry_run or args.t) and (len(observations) > 0):
        try:
            catch.add_observations(observations)
        except:
            logger.error("A fatal error occurred saving data to the database.",
                         exc_info=True)
            raise

    logger.info('%d files processed.', tri.i)

    if failed > 0:
        logger.warning("Failed processing %d files", failed)

    if not (args.dry_run or args.t):
        logger.info("Updating survey statistics.")
        for source in ("atlas_mauna_loa", "atlas_haleakela",
                       "atlas_rio_hurtado", "atlas_sutherland"):
            catch.update_statistics(source=source)
        logger.info('Consider database vacuum.')
