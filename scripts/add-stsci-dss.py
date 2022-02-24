# Licensed with the 3-clause BSD license.  See LICENSE for details.
import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS

from catch import Catch
from catch.schema import NEATPalomar
from catch.config import Config
from sbsearch.util import FieldOfView, RADec

parser = argparse.ArgumentParser('add-stsci-dss')
parser.add_argument('headers', nargs='*')

args = parser.parse_args()


def pltlabel_to_int_id(pltlabel):
    s = ''
    for c in pltlabel:
        s += c if c.isdigit() else ord(c)
    return int(s)


with Catch(Config.from_file(), save_log=True) as catch:
    obs = []
    for fn in args.headers:
        h = fits.getheader(fn)
        shape = np.array((h['XPIXELS'], h['YPIXELS'])

        wcs = WCS(h)

        v = wcs.all_pix2world([[0, 0], [0, shape[1]], [shape[0], shape[1]],
                               [shape[0], 0]], 0)
        fov = str(FieldOfView(RADec(v, unit='deg')))

        obs.append(STScIDSS(
            id=product_id_to_int_id(label['PLTLABEL']),
            label=label['PLTLABEL'],

            stop
            instrument=label['INSTRUMENT_NAME'],
            jd_start=label['START_TIME'].jd,
            jd_stop=label['STOP_TIME'].jd,
            fov=fov,
            filter=label['FILTER_NAME'],
            exposure=label['EXPOSURE_DURATION'].value,
            airmass=label['AIRMASS']
        ))

    catch.add_observations(obs, update=False)

    catch.update_statistics(source="stsci_dss")
