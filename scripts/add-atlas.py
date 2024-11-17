"""Harvest ATLAS metadata.

* ATLAS data will be continuously archived.
* We have a database that tracks validated collections and the time they were
  validated.
* Check this database to identify new collections to harvest.
* Track harvest runs in a local file.
* Files are fz compressed.
* We just want labels with LIDs ending in .fits.

"""

import os
import sys
import shlex
import logging
import sqlite3
import argparse
from glob import glob
import logging.handlers
from typing import Iterator
from packaging.version import Version

import numpy as np
import astropy.units as u
from astropy.time import Time
from astropy.table import Table
from astropy.coordinates import SkyCoord
import pds4_tools

from catch import Catch, Config as CatchConfig
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


class Config:
    harvest_log_filename: str = "atlas-harvest-log.ecsv"
    harvest_log_format: str = "ascii.ecsv"
    harvest_source: str = "atlas"
    logger_name: str = "CATCH/Add ATLAS"


class LabelError(Exception):
    pass


class CornerTestFail(Exception):
    pass


def get_logger():
    return logging.getLogger(Config.logger_name)


def setup_logger(log_filename):
    if not os.path.exists(os.path.dirname(log_filename)):
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    logger = logging.getLogger(Config.logger_name)
    logger.setLevel(logging.INFO)

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(levelname)s:%(name)s:%(asctime)s: %(message)s")

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG if args.verbose else logging.ERROR)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(log_filename)
    handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("Initialized.")
    logger.debug(f"astropy {astropy_version}")
    logger.debug(f"catch {catch_version}")
    logger.debug(f"pds4_tools {pds4_tools_version}")
    logger.debug(f"sbpy {sbpy_version}")
    logger.debug(f"sbsearch {sbsearch_version}")
    logger.info("%s", " ".join([shlex.quote(s) for s in sys.argv]))

    return logger


def get_validation_database(fn) -> sqlite3.Connection:
    logger = get_logger()

    try:
        db = sqlite3.connect(f"file:{fn}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
    except Exception as exc:
        logger.error("Could not connect to database %s", fn)
        raise exc

    return db


def get_harvest_log() -> Table:
    tab: Table
    if os.path.exists(Config.harvest_log_filename):
        tab = Table.read(Config.harvest_log_filename, format=Config.harvest_log_format)
    else:
        tab = Table(
            names=[
                "start",
                "end",
                "source",
                "time_of_last",
                "files",
                "added",
                "duplicates",
                "errors",
            ],
            dtype=["<U23", "<U23", "<U32", "<U23", int, int, int],
        )

    os.system(
        f"cp -f --backup=numbered {Config.harvest_log_filename} {Config.harvest_log_filename}"
    )

    sixth_backup = Config.harvest_log_filename + ".~6~"
    if os.path.exists(sixth_backup):
        os.unlink(sixth_backup)

    return tab


def write_harvest_log(tab: Table, dry_run: bool) -> None:
    if dry_run:
        return

    tab.write(
        Config.harvest_log_filename, format=Config.harvest_log_format, overwrite=True
    )


def get_time_of_last(tab: Table) -> Time:
    """Get the time of the last file validation."""
    last_run = np.argsort(tab[tab["source"] == Config.harvest_source]["end"])[-1]
    return Time(tab[last_run]["time_of_last"])


def is_harvest_processing(tab: Table) -> bool:
    """True if a harvester log entry is set to 'processing'."""
    is_processing = any(
        tab[tab["source"] == Config.harvest_source]["end"] == "processing"
    )
    return is_processing


def get_collections(db, start, stop):
    """Get collections validated between start and stop.

    The rows are ordered so that if a fatal error occurs the next run might be
    able to recover.

    """

    cursor = db.execute(
        """SELECT * FROM nn
           WHERE current_status = 'validated'
             AND recorded_at > ? AND recorded_at < ?
           ORDER BY recorded_at
        """,
        (start.unix, stop.unix),
    )
    return list(cursor.fetchall())


def collection_version(collection) -> Version:
    """Get the collection version."""
    is_collection = (
        collection.label.find("Identification_Area/product_class").text
        == "Product_Collection"
    )
    vid = collection.label.find("Identification_Area/version_id")
    if not is_collection or vid is None:
        raise LabelError("This does not appear to be a valid PDS4 label.")
    return Version(vid.text)


def get_lidvid(label):
    """Return the LIDVID."""
    lid = label.find("Identification_Area/logical_identifier").text
    vid = label.find("Identification_Area/version_id").text
    return "::".join((lid, vid))


def get_image_labels(collection, data_directory) -> Iterator:
    """Iterator of image files to ingest.

    The label file names for all LIDVIDs ending with ".fits" in the collection
    inventory will be returned.   (Do not add .diff files to CATCH.)

    Candidate labels are collected from xml files within `data_directory`.

    """

    logger = get_logger()

    # get lidvids of images from the collection inventory
    fits_inventory = {
        lidvid for lidvid in collection[0].data["LIDVID_LID"] if ".fits::" in lidvid
    }

    # yield all .fits.xml labels in the data directory with lidvids in the
    # fits_inventory
    for fn in glob(f"{data_directory}/*.fits.xml"):
        label = pds4_tools.read(fn, quiet=True, lazy_load=True).label
        lidvid = get_lidvid(label)
        if lidvid in fits_inventory:
            fits_inventory -= set([lidvid])
            yield label
        else:
            raise LabelError(f"Not found in collection inventory: {lidvid}")

    for lidvid in fits_inventory:
        logger.error("%s not found in %s", lidvid, data_directory)


def get_obs_model(lid):
    # example LID: urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
    tel = lid.split(":")[-1][:2]
    return {
        "01": ATLASMaunaLoa,
        "02": ATLASHaleakela,
        "03": ATLASSutherland,
        "04": ATLASRioHurtado,
    }[tel]


def process(label):
    lid = label.find("Identification_Area/logical_identifier").text

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

    # is there a diff image?
    derived_lids = label.findall(
        "Reference_List/Internal_Reference[reference_type='data_to_derived_product']/lid_reference"
    )
    expected_diff_lid = lid[:-4] + "diff"  # replace fits with diff
    obs.diff = any(
        [derived_lid.text == expected_diff_lid for derived_lid in derived_lids]
    )

    # verify corner order
    coords1 = SkyCoord(ra, dec, unit="deg")
    coords2 = SkyCoord(np.roll(ra, 1), np.roll(dec, 1), unit="deg")
    c = coords1.cartesian.cross(coords2.cartesian)
    test = np.sqrt(np.sum(c.xyz.sum(1) ** 2))
    # expecting a value ~0.02, if it is much smaller then there is an issue
    if test < 0.01:
        raise CornerTestFail("Corner test failure: " + get_lidvid(label))

    return obs


parser = argparse.ArgumentParser()
parser.add_argument(
    "file",
    type=os.path.normpath,
    help="ATLAS-PDS processing database or PDS4 label to test (with --test)",
)
parser.add_argument(
    "--config",
    default="catch.config",
    type=CatchConfig.from_file,
    help="CATCH configuration file",
)

mutex = parser.add_mutually_exclusive_group()
mutex.add_argument(
    "--since-date", type=Time, help="harvest metadata validated since this date"
)
mutex.add_argument(
    "--past",
    type=int,
    help="harvest metadata validated in the past SINCE hours",
)
mutex.add_argument(
    "--between-dates",
    type=Time,
    nargs=2,
    help="harvest metadata validated between these dates",
)

parser.add_argument(
    "--only-process",
    metavar="LID",
    help="only process the collection matching this LID",
)
parser.add_argument(
    "--log", default="./logging/add-atlas.log", help="log messages to this file"
)
parser.add_argument(
    "--dry-run",
    "-n",
    action="store_true",
    help="process labels, but do not add to the database",
)
parser.add_argument(
    "--test", action="store_true", help="<file> is a PDS4 ATLAS label to test"
)
parser.add_argument(
    "--verbose", "-v", action="store_true", help="log debugging messages"
)

args = parser.parse_args()

if args.test:
    label = pds4_tools.read(args.file, quiet=True, lazy_load=True).label
    print(process(label))
    sys.exit()

logger = setup_logger(args.log)

if args.verbose:
    logger.setLevel(logging.DEBUG)

if args.dry_run:
    logger.info("Dry run, databases will not be updated.")

validation_db = get_validation_database(args.file)

harvest_log = get_harvest_log()
if is_harvest_processing(harvest_log):
    logger.error('Harvester log state is "processing"')
    sys.exit(1)

start: Time
stop: Time = Time.now().iso
if args.between_dates is not None:
    start = args.between_dates[0]
    stop = args.between_dates[-1]
    logger.info(
        "Checking for collections validated between %s and %s", start.iso, stop.iso
    )
elif args.past is not None:
    start = Time.now() - args.past * u.hr
    logger.info(
        "Checking for collections validated in the past %d hr (since %s)",
        args.past,
        start.iso,
    )
elif args.since_date:
    start = args.since_date
    logger.info("Checking for collections validated since %s", start.iso)
else:
    start = get_time_of_last(harvest_log)

results = get_collections(validation_db, start, stop)

if len(results) == 0:
    logger.info("No new data collections found.")
else:
    with Catch.with_config(args.config) as catch:
        harvest_log.add_row(
            {
                "start": Time.now().iso,
                "end": "processing",
                "source": Config.harvest_source,
                "time_of_last": "",
                "files": 0,
                "added": 0,
                "errors": 0,
            }
        )

        write_harvest_log(harvest_log, args.dry_run)

        for i, row in enumerate(results):
            logger.info("%d collections to process.", len(results) - i)

            collections = [
                pds4_tools.read(fn, quiet=True, lazy_load=True)
                for fn in glob(f"/n/{row['location']}/collection_{row['nn']}*.xml")
            ]

            # find the latest collection lidvid and save to the log
            versions = [collection_version(label) for label in collections]
            latest = collections[versions.index(max(versions))]
            lid = latest.label.find("Identification_Area/logical_identifier").text
            vid = latest.label.find("Identification_Area/version_id").text

            if args.only_process is not None and lid != args.only_process:
                continue

            # Find image products in the data directory
            data_directory = os.path.normpath(f"/n/{row['location']}/data")
            logger.debug(
                "Inspecting directory %s for image products",
                data_directory,
            )

            logger.info("%s::%s, %s", lid, vid, data_directory)

            # harvest metadata
            added = 0
            duplicates = 0
            errors = 0
            observations = []
            tri: ProgressTriangle = ProgressTriangle(1, logger)
            for label in get_image_labels(latest, data_directory):
                tri.update()
                try:
                    obs = process(label)

                    # was this already in the database?
                    if catch.session.query(
                        type(obs).product_id == obs.product_id
                    ).exists():
                        duplicates += 1
                        continue

                    observations.append(obs)
                    added += 1
                except Exception as exc:
                    logger.error(exc)
                    errors += 1

            if not args.dry_run:
                catch.add_observations(observations)

            logger.info("%d files processed", tri.i)
            logger.info("%d files added", added)
            logger.info("%d files already in the database", added)
            logger.info("%d files errored", errors)
            tri.done()

            # update harvest log
            harvest_log[-1]["files"] += tri.i
            harvest_log[-1]["added"] += added
            harvest_log[-1]["duplicates"] += duplicates
            harvest_log[-1]["errors"] += errors
            harvest_log[-1]["time_of_last"] = max(
                harvest_log[-1]["time_of_last"],
                Time(row["recorded_at"], format="unix").iso,
            )
            write_harvest_log(harvest_log, args.dry_run)

        logger.info("Processing complete.")
        logger.info("%d files processed", harvest_log[-1]["files"])
        logger.info("%d files added", harvest_log[-1]["added"])
        logger.info("%d files already in the database", harvest_log[-1]["duplicates"])
        logger.info("%d files errored", harvest_log[-1]["errors"])

        harvest_log[-1]["end"] = Time.now().iso
        write_harvest_log(harvest_log, args.dry_run)

        if not args.dry_run:
            logger.info("Updating survey statistics.")
            for source in (
                "atlas_mauna_loa",
                "atlas_haleakela",
                "atlas_rio_hurtado",
                "atlas_sutherland",
            ):
                catch.update_statistics(source=source)

logger.info("Finished.")
