"""Harvest Spacewatch metadata from PSI.

Label file names, two formats:
https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/2003/03/23/sw_0993_09.01_2003_03_23_09_18_47.001.xml
https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/2008/10/31/sw_1062_K03W25B_2008_10_31_12_00_52.005.xml

Can be derived from the LIDs:
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_0993_09.01_2003_03_23_09_18_47.001.fits
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1062_k03w25b_2008_10_31_12_00_52.005.fits

And the LIDs may be found in the collection inventory:
https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv

"""

import os
import argparse
import logging

import requests
from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.spacewatch import Spacewatch
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from requests import __version__ as requests_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


# URL for the latest list of all files.
INVENTORY = (
    "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv"
)

# URL prefix for the archive at PSI
ARCHIVE_PREFIX = "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data"

def setup_logger(log_filename):
    logger = logging.getLogger("add-spacewatch")
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler(log_filename))
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
    return logger

def inventory():
    """Download inventory as needed, iterate over all files of interest.

    Returns
    -------
    labels : iterator
        Label URLs.

    """

    logger = logging.getLogger("add-spacewatch")
    local_filename = "collection_gbo.ast.spacewatch.survey_data_inventory.csv"

    if os.path.exists(local_filename):
        logger.info(
            "Using previously downloaded inventory list %s.", local_filename
        )
    else:
        with requests.get(INVENTORY, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info("Downloaded inventory list.")

    with open(local_filename, 'r') as inf:
        for line in inf:
            if not line.startswith('P,urn:nasa:pds:spacewatch_mosaic_survey:data:sw_'):
                continue
            if '.fits' not in line:
                continue

            lid = line[2:-6].strip()
            basename = lid.split(':')[-1][:-5]

            # upper case for all but the sw prefix
            basename = basename.upper()
            basename = 'sw' + basename[2:]

            parts = basename.split('_')
            year = parts[3]
            month = parts[4]
            day = parts[5]
            
            url = f"{ARCHIVE_PREFIX}/{year}/{month}/{day}/{basename}.xml"
            yield url

def process(url):
    label = pds4_read(url, lazy_load=True, quiet=True).label
    lid = label.find("Identification_Area/logical_identifier").text
    obs = Spacewatch(
        product_id = lid,
        mjd_start = Time(
            label.find("Observation_Area/Time_Coordinates/start_date_time").text
        ).mjd,
        mjd_stop = Time(
            label.find("Observation_Area/Time_Coordinates/stop_date_time").text
        ).mjd,
        exposure = float(label.find(".//img:Exposure/img:exposure_duration").text),
        filter = label.find(".//img:Optical_Filter/img:filter_name").text,
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
args = parser.parse_args()

logger = setup_logger(args.log)

if args.dry_run:
    logger.info("Dry run, databases will not be updated.")

if args.v:
    logger.setLevel(logging.DEBUG)

with Catch.with_config(args.config) as catch:
    observations = []
    failed = 0

    tri = ProgressTriangle(1, logger=logger, base=2)
    for url in inventory():
        try:
            observations.append(process(url))
            msg = "added"
        except ValueError as e:
            failed += 1
            msg = str(e)
        except:
            logger.error(
                "A fatal error occurred processing %s", url, exc_info=True
            )
            raise

        logger.debug("%s: %s", url, msg)
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

    logger.info("Updating survey statistics.")
    catch.update_statistics(source="spacewatch")

print('Consider database vacuum.')
