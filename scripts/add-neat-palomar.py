# Licensed with the 3-clause BSD license.  See LICENSE for details.
import os
from glob import glob
import argparse
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
import pds3

from catch import Catch
from catch.schema import NEATPalomar
from catch.config import Config
from sbsearch.util import FieldOfView, RADec

parser = argparse.ArgumentParser('add-neat-palomar')
parser.add_argument('path', help='directory containing NEAT PDS3 labels (.lbl suffix)')

args = parser.parse_args()


def product_id_to_int_id(pid):
    s = pid.split('_')[-1]
    s = s[:-1] + str(ord(s[-1]) - 65)
    return int(s[2:])


with Catch(Config.from_file(), save_log=True) as catch:
    obs = []
    for labelfn in glob(os.path.join(args.path, '*.lbl')):
        path = os.path.dirname(labelfn)
        label = pds3.PDS3Label(labelfn)

        # local archive has compressed data:
        datafn = os.path.join(path, label['^IMAGE'][0]) + '.fz'
        h = fits.getheader(datafn, ext=1)

        # hardcoded because Palomar Tricam part 1 labels are wrong
        # shape = np.array((label['IMAGE']['LINES'],
        #                   label['IMAGE']['SAMPLES']))
        shape = np.array((4080, 4080))

        wcs = WCS(naxis=2)
        try:
            wcs.wcs.ctype = h['CTYPE1'], h['CTYPE2']
            wcs.wcs.crval = h['CRVAL1'], h['CRVAL2']
            wcs.wcs.crpix = h['CRPIX1'], h['CRPIX2']
            wcs.wcs.cdelt = h['CDELT1'], h['CDELT2']
        except KeyError:
            continue

        v = wcs.all_pix2world([[0, 0], [0, shape[1]], [shape[0], shape[1]],
                               [shape[0], 0]], 0)
        fov = str(FieldOfView(RADec(v, unit='deg')))

        obs.append(NEATPalomar(
            id=product_id_to_int_id(label['PRODUCT_ID']),
            productid=label['PRODUCT_ID'],
            instrument=label['INSTRUMENT_NAME'],
            jd_start=label['START_TIME'].jd,
            jd_stop=label['STOP_TIME'].jd,
            fov=fov,
            filter=label['FILTER_NAME'],
            exposure=label['EXPOSURE_DURATION'].value,
            airmass=label['AIRMASS']
        ))

    catch.add_observations(obs, update=False)
