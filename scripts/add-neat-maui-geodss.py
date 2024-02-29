# Licensed with the 3-clause BSD license.  See LICENSE for details.
import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
import pds3

from catch import Catch
from catch.model import NEATMauiGEODSS
from catch.config import Config

parser = argparse.ArgumentParser("add-neat-maui-geodss")
parser.add_argument("path")
parser.add_argument("-r", action="store_true", help="recursive search")
parser.add_argument("--config", help="CATCH configuration file")
# parser.add_argument('-u', action='store_true', help='update')

args = parser.parse_args()


def product_id_to_int_id(pid):
    s = pid.split("_")[-1]
    s = s[:-1] + str(ord(s[-1]) - 65)
    return int(s)


with Catch.with_config(Config.from_file(args.config)) as catch:
    for path, dirnames, filenames in os.walk(args.path):
        catch.logger.info("inspecting " + path)
        observations = []
        labels = [f for f in filenames if f.endswith(".lbl")]
        for labelfn in labels:
            try:
                label = pds3.PDS3Label(os.path.join(path, labelfn))
            except:
                catch.logger.error("unable to read " + labelfn)
                continue

            if label["PRODUCT_NAME"] != "NEAT GEODSS IMAGE":
                catch.logger.warning("not a GEODSS image label: " + labelfn)
                continue

            # local archive has compressed data:
            datafn = os.path.join(path, label["^IMAGE"][0]) + ".fz"
            h = fits.getheader(datafn, ext=1)

            # skip dark frames:
            if h["SHUTTER"] == "CLOSED":
                continue

            shape = np.array((label["IMAGE"]["LINES"], label["IMAGE"]["LINE_SAMPLES"]))

            wcs = WCS(naxis=2)
            try:
                wcs.wcs.ctype = h["CTYPE1"], h["CTYPE2"]
                wcs.wcs.crval = h["CRVAL1"], h["CRVAL2"]
                wcs.wcs.crpix = h["CRPIX1"], h["CRPIX2"]
                wcs.wcs.cdelt = h["CDELT1"], h["CDELT2"]
            except KeyError:
                continue

            ra, dec = wcs.all_pix2world(
                [[0, 0], [0, shape[1]], [shape[0], shape[1]], [shape[0], 0]], 0
            ).T

            obs = NEATMauiGEODSS(
                source_id=product_id_to_int_id(label["PRODUCT_ID"]),
                product_id=label["PRODUCT_ID"],
                mjd_start=label["START_TIME"].mjd,
                mjd_stop=label["STOP_TIME"].mjd,
                filter=label["FILTER_NAME"],
                exposure=label["EXPOSURE_DURATION"].to_value("s"),
                airmass=label["AIRMASS"] if label["AIRMASS"] != "UNK" else None,
            )

            if label["START_TIME"].mjd == label["STOP_TIME"].mjd:
                # As of 2022 Feb 07, there are four known files with this error
                catch.logger.warning(
                    f'START_TIME == STOP_TIME for {label["PRODUCT_ID"]}.  '
                    "Setting STOP_TIME = START_TIME + EXPOSURE_DURATION."
                )
                obs.mjd_stop = obs.mjd_start + obs.exposure / 86400

            obs.set_fov(ra, dec)
            observations.append(obs)

        catch.add_observations(observations)

        if not args.r:
            break

    catch.update_statistics(source="neat_maui_geodss")
