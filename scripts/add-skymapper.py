# Licensed with the 3-clause BSD license.  See LICENSE for details.
import re
import argparse
import gzip
import csv

import numpy as np

from sbsearch.util import FieldOfView, RADec
from sbsearch.logging import ProgressTriangle
from catch import Catch
from catch.schema import SkyMapper
from catch.config import Config

parser = argparse.ArgumentParser(
    'add-skymapper', description='Add SkyMapper DR2 metadata to CATCH.')
parser.add_argument('image_table', help='image (exposure) table')
parser.add_argument('ccd_table', help='ccd table')
parser.add_argument(
    '--config', help='CATCH configuration file')
parser.add_argument(
    '--debug', action='store_true', help='debug mode')

args = parser.parse_args()


def get_rows(filename):
    """Opens CSV file, may be gzipped, returns context object"""
    # is the file gzipped?
    with open(filename, 'rb') as inf:
        gzipped = inf.read(2) == bytes.fromhex('1f8b')

    if gzipped:
        csvfile = gzip.open(filename, 'rt')
    else:
        csvfile = open(filename, 'r')

    # process CCD table with csv module for efficiency
    for row in csv.DictReader(csvfile):
        yield row

    csvfile.close()


def cov2fov(cov):
    """Convert coverage string from CCD table to FieldOfView object."""
    v = (np.array(re.findall(r'[0-9e\.+-]+', cov.replace(' ', '')), float)
         .reshape((4, 2)))
    return str(FieldOfView(RADec(v, unit='rad')))


# read in images table, sort into a dictionary keyed by image_id
images = {}
for row in get_rows(args.image_table):
    # image_id,night_mjd,date,ra,decl,field_id,exp_time,airmass,filter,
    # rotator_pos,object,image_type,fwhm,elong,background,zpapprox
    k = row['image_id']
    images[k] = {}
    for col in row.keys():
        images[k][col] = row[col]

# connect to catch database
with Catch(Config.from_args(args), save_log=True, debug=False) as catch:
    obs = []
    count = 0
    tri = ProgressTriangle(1, catch.logger, base=2)
    # iterate over rows in CCD table
    for row in get_rows(args.ccd_table):
        # image_id,ccd,filename,maskname,image,filter,mjd_obs,fwhm,elong,
        # nsatpix,sb_mag,phot_nstar,header,coverage
        image = images[row['image_id']]
        fov = cov2fov(row['coverage'])
        sb_mag = None if row['sb_mag'] == '' else float(row['sb_mag'])

        # SkyMapper object inherits sbsearch.schema.Obs columns.
        # Note that 'source' and 'obsid' will be defined via sqlalchemy's polymorphism machinery.
        obs.append(SkyMapper(
            id=int(row['image'].replace('-', '')),
            jd_start=float(row['mjd_obs']) + 2400000.5,
            jd_stop=float(row['mjd_obs']) + 2400000.5 +
            float(image['exp_time']) / 86400,
            fov=fov,
            filter=row['filter'],
            exposure=float(image['exp_time']),
            seeing=float(row['fwhm_ccd']),
            airmass=image['airmass'],
            productid=row['image'],
            sb_mag=sb_mag,
            field_id=int(image['field_id']),
            image_type=image['image_type'],
            zpapprox=image['zpapprox']
        ))

        # add 10000 at a time
        tri.update()
        if tri.i % 10000 == 0:
            catch.add_observations(obs, update=False)
            obs = []

    # add any remaining files
    catch.add_observations(obs, update=False)
