"""Harvest Spacewatch metadata from PSI.

Label file names, two formats:
gbo.ast.spacewatch.survey/data/2003/03/23/sw_0993_09.01_2003_03_23_09_18_47.001.xml
gbo.ast.spacewatch.survey/data/2008/10/31/sw_1062_K03W25B_2008_10_31_12_00_52.005.xml

Can be derived from the LIDs:
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_0993_09.01_2003_03_23_09_18_47.001.fits
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1062_k03w25b_2008_10_31_12_00_52.005.fits

And the LIDs may be found in the collection inventory:
gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv

Download all data labels and the collection inventory to a local directory:

wget -r -R *.fits --no-parent https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/

"""

import os
import argparse
import logging
from glob import iglob

from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.spacewatch import Spacewatch
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


def setup_logger(log_filename):
    logger = logging.getLogger("add-spacewatch")
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
    """Iterate over all files of interest.

    Returns
    -------
    labels : iterator of tuples
        Path and pds4_tools label object.

    """

    logger = logging.getLogger("add-spacewatch")
    inventory_fn = f"{base_path}/gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv"

    if not os.path.exists(base_path):
        raise Exception('Missing inventory list %s', fn)

    # Read in all relevant LIDs from the inventory.
    lids = set()
    with open(inventory_fn, 'r') as inf:
        for line in inf:
            if not line.startswith('P,urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_'):
                continue
            if '.fits' not in line:
                continue

            lid = line[2:-6]
            lids.add(lid)

    # search directory-by-directory for labels with those LIDs
    for fn in iglob(f"{base_path}/gbo.ast.spacewatch.survey/data/20*/*/*/*.xml"):
        label = pds4_read(fn, lazy_load=True, quiet=True).label
        lid = label.find("Identification_Area/logical_identifier").text
        if lid in lids:
            lids.remove(lid)
            yield fn, label

    # did we find all the labels?
    if len(lids) > 0:
        logger.error(f'{len(lids)} LIDs were not found.')


def process(fn, label):
    lid = label.find("Identification_Area/logical_identifier").text
    obs = Spacewatch(
        product_id=lid,
        mjd_start=Time(
            label.find("Observation_Area/Time_Coordinates/start_date_time").text
        ).mjd,
        mjd_stop=Time(
            label.find("Observation_Area/Time_Coordinates/stop_date_time").text
        ).mjd,
        exposure=float(label.find(
            ".//img:Exposure/img:exposure_duration").text),
        filter=label.find(".//img:Optical_Filter/img:filter_name").text,
        label=fn[fn.index('gbo.ast.spacewatch.survey'):]
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

    maglimit = survey.find(".//survey:Rollover/survey:rollover_magnitude")
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    return obs


parser = argparse.ArgumentParser(
    description="Add Spacewatch data to CATCH.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument('base_path', help="path to data collection root directory")
parser.add_argument(
    "--config",
    default="catch.config",
    type=Config.from_file,
    help="CATCH configuration file",
)
parser.add_argument("--log", default="add-spacewatch.log", help="log file")
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

if not os.path.exists(f"{args.base_path}/gbo.ast.spacewatch.survey"):
    raise ValueError(
        f"gbo.ast.spacewatch.survey not found in {args.base_path}")

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
        catch.update_statistics(source="spacewatch")
        logger.info('Consider database vacuum.')
