# Licensed with the 3-clause BSD license.  See LICENSE for details.
import re
import argparse
import gzip
import csv

import numpy as np

from sbsearch.logging import ProgressTriangle
from catch import Catch
from catch.model import SkyMapperDR4
from catch.config import Config

parser = argparse.ArgumentParser(
    "add-skymapper", description="Add SkyMapper DR4 metadata to CATCH."
)
parser.add_argument("image_table", help="image (exposure) table")
parser.add_argument("ccd_tables", nargs="+", help="ccd tables")
parser.add_argument("--config", help="CATCH configuration file")
parser.add_argument("--debug", action="store_true", help="debug mode")
parser.add_argument(
    "--noop", action="store_true", help="no-operation mode, just process the files"
)

args = parser.parse_args()


def get_rows(filename):
    """Opens CSV file, may be gzipped, returns context object"""
    # is the file gzipped?
    with open(filename, "rb") as inf:
        gzipped = inf.read(2) == bytes.fromhex("1f8b")

    if gzipped:
        csvfile = gzip.open(filename, "rt")
    else:
        csvfile = open(filename, "r")

    # process CCD table with csv module for efficiency
    for row in csv.DictReader(csvfile):
        yield row

    csvfile.close()


def cov2fov(cov):
    """Convert coverage string from CCD table to ra, dec."""
    fov = (
        np.array(re.findall(r"[0-9e\.+-]+", cov.replace(" ", "")), float).reshape(
            (4, 2)
        )
    ).T
    return fov


# read in images table, sort into a dictionary keyed by image_id
images = {}
for row in get_rows(args.image_table):
    # image_id,night_mjd,date,ra,decl,field_id,exp_time,airmass,filter,
    # rotator_pos,object,image_type,fwhm,elong,background,zpapprox
    k = row["image_id"]
    images[k] = {}
    for col in row.keys():
        images[k][col] = row[col]

# connect to catch database
with Catch.with_config(Config.from_args(args)) as catch:
    if args.noop:
        catch.logger.info(
            "No-operation mode; not adding files to the database or computing spatial indices."
        )
    else:
        catch.db.drop_spatial_index()

    observations = []
    count = 0
    # iterate over files
    for ccd_table in args.ccd_tables:
        tri = ProgressTriangle(1, catch.logger, base=2)
        # iterate over rows in CCD table
        for row in get_rows(ccd_table):
            # image_id,ccd,filename,maskname,image,filter,mjd_obs,fwhm,elong,
            # nsatpix,sb_mag,phot_nstar,header,coverage
            image = images[row["image_id"]]
            ra, dec = np.degrees(cov2fov(row["coverage"]))
            sb_mag = None if row["sb_mag"] == "" else float(row["sb_mag"])
            field_id = int(image["field_id"]) if image["field_id"] != "" else None

            # SkyMapperDR4 object inherits sbsearch.schema.Obs columns.
            # Note that 'source' and 'obsid' will be defined via sqlalchemy's polymorphism machinery.
            obs = SkyMapperDR4(
                source_id=int(row["image"].replace("-", "")),
                mjd_start=float(row["mjd_obs"]),
                mjd_stop=float(row["mjd_obs"]) + float(image["exp_time"]) / 86400,
                filter=row["filter"],
                exposure=float(image["exp_time"]),
                seeing=float(row["fwhm_ccd"]),
                airmass=image["airmass"],
                product_id=row["image"],
                sb_mag=sb_mag,
                field_id=field_id,
                image_type=image["image_type"],
                zpapprox=image["zpapprox"],
            )
            obs.set_fov(ra, dec)
            observations.append(obs)

            # add 10000 at a time
            tri.update()
            count += 1
            if tri.i % 10000 == 0:
                if not args.noop:
                    catch.add_observations(observations)
                observations = []

        # add any remaining files
        if not args.noop:
            catch.add_observations(observations)

    if not args.noop:
        catch.db.create_spatial_index()
        catch.update_statistics(source="skymapper_dr4")

    catch.logger.info(f"Processed %d images.", count)
