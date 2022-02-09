from contextlib import contextmanager
import re
import argparse
import logging
import sqlite3
import requests
from astropy.time import Time

from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.catalina import CatalinaBigelow, CatalinaKittPeak, CatalinaLemmon

# URL for the latest list of all files.
LATEST_FILES = (
    "https://sbnarchive.psi.edu/pds4/surveys/catalina_extras/file_list.latest.txt"
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


def get_new_list():
    """Download a new file list.

    Downloaded files are renamed using the time of the download.

    Returns
    -------
    listfile : str
        The name of the file.

    """

    date = Time.now().isot.replace(":", "").replace("-", "")[:13]
    local_filename = f"css-file-list-{date}.txt"
    with requests.get(LATEST_FILES, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


def new_labels(db, listfile):
    """Check for new labels to add.


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
    with open(listfile, "r") as inf:
        for line in inf:
            line_count += 1
            if re.match(".*data_calibrated/.*\.xml\n$", line):
                if "collection" in line:
                    continue
                calibrated_count += 1
                path = line.strip()
                path = path[line.find("gbo.ast.catalina.survey") :]
                processed = db.execute(
                    "SELECT TRUE FROM labels WHERE path = ?", (path,)
                ).fetchone()
                if processed is None:
                    processed_count += 1
                    yield path
                    break

    logger = logging.getLogger("add-css")
    logger.info(
        f"""
Processed:
  {line_count} lines
  {calibrated_count} calibrated data labels
  {processed_count} new files
"""
    )


def process(path):
    url = "".join((ARCHIVE_PREFIX, path))
    label = pds4_read(url, lazy_load=True, quiet=True).label
    lid = label.find("Identification_Area/logical_identifier").text
    tel = lid.split(":")[5][:3]
    if tel in CatalinaBigelow._telescopes:
        obs = CatalinaBigelow()
    elif tel in CatalinaLemmon._telescopes:
        obs = CatalinaLemmon()
    elif tel in CatalinaKittPeak._telescopes:
        obs = CatalinaKittPeak()
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
    formatter = logging.Formatter("%(levelname)s %(asctime)s (%(name)s): %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    if args.dry_run:
        logger.info("Dry run, databases will not be updated.")

    if args.v:
        logger.setLevel(logging.DEBUG)

    if args.f is None:
        listfile = get_new_list()
        logger.info("New CSS file list downloaded.")
    else:
        listfile = args.f

    with harvester_db(args.db) as db:
        with Catch.with_config(args.config) as catch:
            observations = []
            failed = 0
            for path in new_labels(db, listfile):
                try:
                    observations.append(process(path))
                    msg = "added"
                except ValueError as e:
                    failed += 1
                    msg = str(e)
                except:
                    logger.error(
                        "A fatal error occurred processing %s", path, exc_info=True
                    )

                logger.debug("%s: %s", path, msg)

                if args.dry_run:
                    continue

                db.execute(
                    "INSERT INTO labels VALUES (?,?,?)", (path, Time.now().iso, msg)
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


if __name__ == "__main__":
    main()
