# Licensed with the 3-clause BSD license.  See LICENSE for details.
import os
import sys
import argparse
import sqlite3

import numpy as np
from astropy.io import fits
from astropy.wcs import WCS

from sbsearch.logging import ProgressTriangle
from catch import Catch, Config
from catch.model import PS1DR2

parser = argparse.ArgumentParser(
    "add-ps1", description="Add PS1 DR2 metadata to CATCH."
)
parser.add_argument("warp_meta", help="warp (single-epochal image) metadata")
parser.add_argument("warp_files", help="warp file name table")
parser.add_argument("ps1_grid", help="grid specifications")
parser.add_argument(
    "--db-only",
    action="store_true",
    help="only build temporary database (overwriting current file)",
)
parser.add_argument(
    "--add-only",
    action="store_true",
    help="only add new rows (useful after partial update)",
)
parser.add_argument(
    "--update",
    action="store_true",
    help="update existing rows (edit script for data to update), and add missing rows",
)
parser.add_argument(
    "--start-offset",
    type=int,
    default=0,
    help="start inserting after skipping this many rows (an alterative to --add-only)",
)
parser.add_argument("-n", type=int, help="only add n observations to catch")
parser.add_argument("--config", help="CATCH configuration file")
parser.add_argument("--debug", action="store_true", help="debug mode")

args = parser.parse_args()


sqlite3.register_adapter(np.int64, int)
sqlite3.register_adapter(np.int32, int)
sqlite3.register_adapter(np.int16, int)
sqlite3.register_adapter(np.uint8, int)
sqlite3.register_adapter(np.float64, float)

filters = {1: "gp1", 2: "rp1", 3: "ip1", 4: "zp1", 5: "yp1"}

# https://outerspace.stsci.edu/display/PANSTARRS/PS1+Sky+tessellation+patterns
image_sizes = {}
with fits.open(args.ps1_grid) as hdu:
    # Zone is the declination row number, which starts at 0 at δ = −90°.
    # ProjCell is the projection cell number for the first cell on that row.
    # M is the number of cells in the row (so the projection cell number runs from ProjCell to ProjCell+M−1).
    # Dec is the declination of the image center.
    # Xsize and Ysize give the size of the full projection cell image in 0.25 arcsec pixels.
    # Xsub and Ysub give the size of the skycell images into which the projection cell is divided for storage in FITS files.
    #
    # Note that Xsub = (Xsize-480)/10+480 pixels.
    # The final two columns, MinDec and MaxDec, give the declination range over which this row is the best choice.
    # These columns are used to select a declination zone in the region where adjacent zones overlap.
    # (The actual algorithm used is a bit more complicated near the north pole, where a simple declination threshold is not sufficient to identify the best projection cell.)
    for row in hdu[1].data:
        image_sizes[row["PROJCELL"]] = (row["YCELL"], row["XCELL"])
projcell_bins = np.sort(list(image_sizes.keys()))


def build_db(warp_meta, warp_files):
    """Create a temporary sqlite3 database with the two source tables."""

    db = sqlite3.connect("ps1dr2.db")
    db.execute(
        """
    CREATE TABLE warp_meta (
        forcedWarpID INTEGER PRIMARY KEY,
        projectionID INTEGER NOT NULL,
        skyCellID INTEGER NOT NULL,
        filterID INTEGER NOT NULL,
        frameID INTEGER NOT NULL,
        telescopeID INTEGER NOT NULL,
        expStart FLOAT NOT NULL,
        expTime FLOAT NOT NULL,
        airmass FLOAT,
        crval1 FLOAT NOT NULL,
        crval2 FLOAT NOT NULL,
        crpix1 FLOAT NOT NULL,
        crpix2 FLOAT NOT NULL
    );
    """
    )
    db.execute(
        """
    CREATE TABLE warp_files (
        filename TEXT,
        mjdobs FLOAT NOT NULL,
        projcell INTEGER NOT NULL,
        skycell INTEGER NOT NULL,
        filterid INTEGER NOT NULL
    );
    """
    )

    meta = fits.open(warp_meta, memmap=True)
    n = 0
    db.execute("BEGIN TRANSACTION;")
    for row in meta[1].data:
        db.execute(
            """
        INSERT INTO warp_meta VALUES (
            :forcedWarpID,
            :projectionID,
            :skyCellID,
            :filterID,
            :frameID,
            :telescopeID,
            :expStart,
            :expTime,
            :airmass,
            :crval1,
            :crval2,
            :crpix1,
            :crpix2
        );
        """,
            row,
        )
    db.execute("END TRANSACTION;")

    db.execute(
        """
    CREATE INDEX warp_meta_index ON warp_meta (
        projectionID, skyCellID, filterID, expStart
    );
    """
    )
    meta.close()
    del meta

    files = fits.open(warp_files, memmap=True)
    db.execute("BEGIN TRANSACTION;")
    for row in files[1].data:
        db.execute(
            """
        INSERT INTO warp_files VALUES (
            :filename,
            :mjdobs,
            :projcell,
            :skycell,
            :filterid
        );
        """,
            row,
        )
    db.execute("END TRANSACTION;")
    files.close()
    del files

    db.execute(
        """
    CREATE INDEX warp_files_index ON warp_files (
        projcell, skycell, filterid, mjdobs
    );
    """
    )

    db.close()


def get_rows(start_offset):
    db = sqlite3.connect("ps1dr2.db")
    db.row_factory = sqlite3.Row
    limit = 10000
    offset = start_offset
    while True:
        # inner join with group by to avoid duplicates in warp_meta
        rows = db.execute(
            """
        SELECT forcedWarpID,projectionID,skyCellID,m.filterID,
          frameID,telescopeID,expStart,expTime,airmass,
          crval1,crval2,crpix1,crpix2,filename
        FROM warp_files AS f
        INNER JOIN warp_meta AS m ON (
            m.projectionID = f.projcell
            AND m.skyCellID = f.skycell
            AND m.filterID = f.filterid
            AND m.expStart = f.mjdobs
        )
        GROUP BY filename
        LIMIT ? OFFSET ?
        """,
            (limit, offset),
        ).fetchall()

        if len(rows) == 0:
            break
        else:
            for row in rows:
                yield row

        offset += limit

    db.close()


if not os.path.exists("ps1dr2.db") or args.db_only:
    print("building temporary database")
    build_db(args.warp_meta, args.warp_files)
    print("completed")

if args.db_only:
    sys.exit(0)

test_time_agreement = False

# connect to catch database
config = Config.from_args(args)
with Catch.with_config(config) as catch:
    # setup WCS object to calculate image corners
    w = WCS(naxis=2)
    w.wcs.ctype = "RA---TAN", "DEC--TAN"
    w.wcs.cdelt = -6.94444461e-05, 6.94444461e-05

    catch.db.drop_spatial_index()

    observations = []
    tri = ProgressTriangle(1, catch.logger, base=2)
    bad_dt = []
    # iterate over rows in temporary database
    for row in get_rows(args.start_offset):
        # PS1DR2 object inherits sbsearch.model.Observation columns
        # observation_id, source, mjd_start, mjd_stop, fov, spatial_terms,
        # filter, exposure, seeing, airmass, maglimit

        # Note that 'source' and 'observation_id' will be defined via
        # sqlalchemy's polymorphism machinery.  fov is updated via a method.
        # spatial_terms are generated upon db insertion.

        if args.add_only or args.update:
            # obs = catch.db.session.query(PS1DR2).filter(
            #     PS1DR2.source_id == row['forcedWarpID']).first()

            # can't use forcedWarpID due to the ~650 double matches between
            # warp_meta and warp_files.
            obs = (
                catch.db.session.query(PS1DR2)
                .filter(PS1DR2.product_id == row["filename"])
                .first()
            )
            if obs is None:
                # create a new row
                obs = PS1DR2()
        else:
            obs = PS1DR2()

        if args.add_only and (obs.source_id is not None):
            continue

        if args.update and obs.source_id is not None:
            # just updating a few things, edit as needed
            raise ValueError("add_your_tasks_here")
        else:
            # forcedWarpID,projectionID,skyCellID,m.filterID as filterID,
            # frameID,telescopeID,expStart,expTime,airmass,
            # crval1,crval2,crpix1,crpix2,filename
            w.wcs.crval = row["crval1"], row["crval2"]
            w.wcs.crpix = row["crpix1"], row["crpix2"]

            i = np.searchsorted(projcell_bins, row["projectionID"], side="right")
            shape = image_sizes[projcell_bins[i - 1]]

            ra, dec = w.all_pix2world(
                [[0, 0], [0, shape[1]], [shape[0], shape[1]], [shape[0], 0]], 0
            ).T

            if obs.source_id is None:
                obs.source_id = row["forcedWarpID"]

            obs.mjd_start = row["expStart"]
            obs.mjd_stop = row["expStart"] + row["expTime"] / 86400
            obs.filter = filters[row["filterID"]]
            obs.exposure = row["expTime"]
            obs.airmass = row["airmass"]
            obs.product_id = row["filename"]
            obs.telescope_id = row["telescopeID"]
            obs.frame_id = row["frameID"]
            obs.projection_id = row["projectionID"]
            obs.skycell_id = row["skyCellID"]
            obs.filter_id = row["filterID"]

            obs.set_fov(ra, dec)

        observations.append(obs)

        # add 10000 at a time
        tri.update()
        if tri.i % 10000 == 0:
            catch.add_observations(observations)
            observations = []

        if args.n is not None and tri.i >= args.n:
            break

    # add any remaining files
    catch.add_observations(observations)
    catch.db.create_spatial_index()
    catch.update_statistics(source="ps1dr2")
