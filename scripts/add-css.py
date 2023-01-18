"""Harvest Catalina Sky Survey metadata from PSI.

As of Feb 2022, CSS data are continuously archived.  This script examines a file
list generated at PSI and downloads new calibrated data labels for metadata
harvesting.

"""

import os
import re
from time import sleep
import email
import urllib
import argparse
import logging
import sqlite3
import gzip
from datetime import datetime
from contextlib import contextmanager

import requests
from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.catalina import CatalinaBigelow, CatalinaBokNEOSurvey, CatalinaLemmon
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from requests import __version__ as requests_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version

# URL for the latest list of all files.
LATEST_FILES = (
    "https://sbnarchive.psi.edu/pds4/surveys/catalina_extras/file_list.latest.txt.gz"
)

# URL prefix for the CSS archive at PSI
ARCHIVE_PREFIX = "https://sbnarchive.psi.edu/pds4/surveys/"


DB_SETUP = (
    """
    CREATE TABLE IF NOT EXISTS labels (
        path TEXT,
        date TEXT,
        status TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS path_index ON labels (path)",
    "CREATE INDEX IF NOT EXISTS date_index ON labels (date)",
    "CREATE INDEX IF NOT EXISTS status_index ON labels (status)",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add CSS data to CATCH.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db", default="add-css.db", help="harvester tracking database"
    )
    parser.add_argument(
        "--config",
        default="catch.config",
        type=Config.from_file,
        help="CATCH configuration file",
    )
    parser.add_argument(
        "-f",
        help="do not download a new file list, but use the provided file name",
    )
    parser.add_argument("--log", default="add-css.log", help="log file")
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="inspect for new files, but do not add to the database",
    )
    parser.add_argument("-v", action="store_true", help="verbose logging")
    return parser.parse_args()


@contextmanager
def harvester_db(filename):
    db = sqlite3.connect(filename)
    try:
        for statement in DB_SETUP:
            db.execute(statement)
        yield db
        db.commit()
    finally:
        db.close()


def sync_list():
    """Check for a new file list and synchronize as needed.


    Returns
    -------
    listfile : str
        The name of the file.

    """

    logger = logging.getLogger("add-css")
    local_filename = "css-file-list.txt.gz"
    sync = False

    if os.path.exists(local_filename):
        # file exists, check for an update
        last_sync = datetime.fromtimestamp(os.stat(local_filename).st_mtime)
        response = requests.head(LATEST_FILES)
        logger.info(
            "Previous file list downloaded %s", last_sync.strftime(
                "%Y-%m-%d %H:%M")
        )
        try:
            file_date = response.headers["Last-Modified"]
            file_date = datetime(*email.utils.parsedate(file_date)[:6])
            logger.info(
                "Online file list dated %s", file_date.strftime(
                    "%Y-%m-%d %H:%M")
            )
            if last_sync < file_date:
                sync = True
                logger.info("New file list available.")
        except KeyError:
            pass
    else:
        # file does not exist, download new file
        sync = True

    if sync:
        with requests.get(LATEST_FILES, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info("Downloaded file list.")

            stat = os.stat(local_filename)
            file_date = Time(stat.st_mtime, format="unix")
            logger.info(f"  Size: {stat.st_size / 1048576:.2f} MiB")
            logger.info(f"  Last modified: {file_date.iso}")

            backup_file = local_filename.replace(
                ".txt.gz",
                "-" +
                file_date.isot[:16].replace(
                    "-", "").replace(":", "") + ".txt.gz",
            )
            os.system(f"cp {local_filename} {backup_file}")

    return local_filename


def new_labels(db, listfile):
    """Iterator for new labels.


    Parameters
    ----------
    db : sqlite3.Connection
        Database of ingested labels (``harvester_db``).

    listfile : str
        Look for new labels in this file.

    Returns
    -------
    path : str

    """

    line_count: int = 0
    calibrated_count: int = 0
    processed_count: int = 0
    with gzip.open(listfile, "rt") as inf:
        for line in inf:
            line_count += 1
            if re.match(".*data_calibrated/.*\.xml\n$", line):
                if "collection" in line:
                    continue
                calibrated_count += 1
                path = line.strip()
                path = path[line.find("gbo.ast.catalina.survey"):]
                processed = db.execute(
                    "SELECT TRUE FROM labels WHERE path = ?", (path,)
                ).fetchone()
                if processed is None:
                    processed_count += 1
                    yield path

    logger = logging.getLogger("add-css")
    logger.info("Processed:")
    logger.info("  %d lines", line_count)
    logger.info("  %d calibrated data labels", calibrated_count)
    logger.info("  %d new files", processed_count)


def process(path, logger):
    url = "".join((ARCHIVE_PREFIX, path))

    attempts = 0
    # address timeout error by retrying with a delay
    while attempts < 4:
        try:
            label = pds4_read(url, lazy_load=True, quiet=True).label
            break
        except urllib.error.URLError as e:
            logger.error(str(e))
            attempts += 1
            sleep(1)  # retry, but not too soon
    else:
        raise e

    lid = label.find("Identification_Area/logical_identifier").text
    tel = lid.split(":")[5][:3].upper()
    if tel in CatalinaBigelow._telescopes:
        obs = CatalinaBigelow()
    elif tel in CatalinaLemmon._telescopes:
        obs = CatalinaLemmon()
    elif tel in CatalinaBokNEOSurvey._telescopes:
        obs = CatalinaBokNEOSurvey()
    else:
        raise ValueError(f"Unknown telescope {tel}")

    obs.product_id = lid
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd
    obs.exposure = round((obs.mjd_stop - obs.mjd_start) * 86400, 3)

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

    maglimit = survey.find(
        "survey:Limiting_Magnitudes"
        "/survey:Percentage_Limit[survey:Percentage_Limit='50']"
        "/survey:limiting_magnitude"
    )
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    return obs


def main():
    args: argparse.Namespace = _parse_args()

    logger = logging.getLogger("add-css")
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler(args.log))
    formatter = logging.Formatter(
        "%(levelname)s %(asctime)s (%(name)s): %(message)s")
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

    if args.dry_run:
        logger.info("Dry run, databases will not be updated.")

    if args.v:
        logger.setLevel(logging.DEBUG)

    if args.f is None:
        listfile = sync_list()
    else:
        listfile = args.f
        logger.info("Checking user-specified file list.")

    with harvester_db(args.db) as db:
        with Catch.with_config(args.config) as catch:
            observations = []
            failed = 0

            tri = ProgressTriangle(1, logger=logger, base=2)
            for path in new_labels(db, listfile):
                try:
                    observations.append(process(path, logger))
                    msg = "added"
                except ValueError as e:
                    failed += 1
                    msg = str(e)
                except:
                    logger.error(
                        "A fatal error occurred processing %s", path, exc_info=True
                    )
                    raise

                logger.debug("%s: %s", path, msg)
                tri.update()

                if args.dry_run:
                    continue

                db.execute(
                    "INSERT INTO labels VALUES (?,?,?)", (path,
                                                          Time.now().iso, msg)
                )

                if len(observations) >= 10000:
                    catch.add_observations(observations)
                    db.commit()
                    observations = []

            # add any remaining files
            if not args.dry_run and (len(observations) > 0):
                catch.add_observations(observations)
                db.commit()

            if failed > 0:
                logger.warning("Failed processing %d files", failed)

            logger.info("Updating survey statistics.")
            for source in ("catalina_bigelow", "catalina_lemmon", "catalina_bokneosurvey"):
                catch.update_statistics(source=source)


if __name__ == "__main__":
    main()
