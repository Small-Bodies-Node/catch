"""Harvest ATLAS metadata.

* ATLAS data will be continuously archived.  Track which files have been
  previously harvested and skip as appropriate.
* Files are fz compressed.
* We just want labels with LIDs ending in .fits.

"""

import os
import argparse
import logging
import sqlite3
from glob import iglob
from datetime import datetime
from contextlib import contextmanager

from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch, Config
from catch.model.atlas import (
    ATLASMaunaLoa,
    ATLASHaleakela,
    ATLASRioHurtado,
    ATLASSutherland,
)
from sbsearch.logging import ProgressTriangle

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


DB_SETUP = (
    """
    CREATE TABLE IF NOT EXISTS labels (
        lid TEXT,
        date TEXT,
        status TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS lid_index ON labels (lid)",
    "CREATE INDEX IF NOT EXISTS date_index ON labels (date)",
    "CREATE INDEX IF NOT EXISTS status_index ON labels (status)",
)


def setup_logger(log_filename):
    logger = logging.getLogger("add-atlas")
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
    logger.debug(f"sbpy {sbpy_version}")
    logger.debug(f"sbsearch {sbsearch_version}")
    return logger


def inventory(base_path, recursive):
    """Find ATLAS labels within this path.

    Labels we want are *.fits.xml  LIDs will end with .fits

    """

    for dirpath, dirnames, filenames in os.walk(base_path):
        for fn in filenames:
            if not fn.endswith(".fits.xml"):
                continue

            path = os.path.join(dirpath, fn)
            label = pds4_read(path, lazy_load=True, quiet=True).label
            lid = label.find("Identification_Area/logical_identifier").text
            if lid.endswith(".fits"):
                # Should be good!
                yield fn, lid, label
        
        if not recursive:
            break

def get_obs_model(lid):
    # example LID: urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
    tel = lid.split(":")[-1][:2]
    return {
        "01": ATLASMaunaLoa,
        "02": ATLASHaleakela,
        "03": ATLASSutherland,
        "04": ATLASRioHurtado,
    }[tel]


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


def process(db, fn, label):
    lid = label.find("Identification_Area/logical_identifier").text

    is_processed = db.execute(
        "SELECT TRUE FROM labels WHERE lid = ?", (lid,)
    ).fetchone()
    if is_processed is not None:
        return None

    obs = get_obs_model(lid)()
    obs.product_id = lid
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd
    obs.exposure = (float(label.find(".//img:Exposure/img:exposure_duration").text),)
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

    obs.field_id = survey.find("survey:field_id").text

    obs.diff = os.path.exists(fn.replace(".fits", ".diff"))

    return obs


parser = argparse.ArgumentParser(
    description="Add ATLAS data to CATCH.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("base_path", help="path to data collection root directory")
parser.add_argument(
    "--db", default="add-atlas.db", help="harvested file tracking database"
)
parser.add_argument("-r", action="store_true", help="recursively add files in subdirectories")
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
    help="just test for the existence of the files (implies -n)",
)

args = parser.parse_args()

logger = setup_logger(args.log)

if args.t:
    logger.info("Testing for existence of all files.")

if args.dry_run or args.t:
    logger.info("Dry run, databases will not be updated.")

if args.v:
    logger.setLevel(logging.DEBUG)

with harvester_db(args.db) as db:
    with Catch.with_config(args.config) as catch:
        observations = []
        added = 0
        failed = 0

        tri = ProgressTriangle(1, logger=logger, base=2)
        db.execute("BEGIN TRANSACTION")
        for fn, lid, label in inventory(args.base_path, args.r):
            tri.update()

            if args.t:
                if not os.path.exists(fn):
                    logger.error("Missing %s", fn)
                continue

            try:
                obs = process(db, fn, label)
                if obs is None:
                    msg = "skipped"
                else:
                    observations.append(obs)
                    msg = "added"
            except ValueError as e:
                failed += 1
                msg = str(e)
            except:
                logger.error("A fatal error occurred processing %s", fn, exc_info=True)
                raise

            logger.debug("%s (%s): %s", fn, lid, msg)

            if args.dry_run or args.t or obs is None:
                continue

            added += 1
            db.execute("INSERT INTO labels VALUES (?,?,?)", (lid, Time.now().iso, msg))
            
            if len(observations) >= 8192:
                try:
                    catch.add_observations(observations)
                except:
                    logger.error(
                        "A fatal error occurred saving data to the database.",
                        exc_info=True,
                    )
                    db.execute("ROLLBACK TRANSACTION")
                    raise
                db.execute("END TRANSACTION")
                db.execute("BEGIN TRANSACTION")
                observations = []

        # add any remaining files
        if not (args.dry_run or args.t) and (len(observations) > 0):
            try:
                catch.add_observations(observations)
            except:
                logger.error(
                    "A fatal error occurred saving data to the database.", exc_info=True
                )
                db.execute("ROLLBACK TRANSACTION")
                raise

        db.execute("END TRANSACTION")

        logger.info("%d files processed.", tri.i)
        logger.info("%d files added.", added)
        logger.info("%d files failed.", failed)

        if failed > 0:
            logger.warning("Failed processing %d files", failed)

        if not (args.dry_run or args.t):
            logger.info("Updating survey statistics.")
            for source in (
                "atlas_mauna_loa",
                "atlas_haleakela",
                "atlas_rio_hurtado",
                "atlas_sutherland",
            ):
                catch.update_statistics(source=source)
            logger.info("Consider database vacuum.")
