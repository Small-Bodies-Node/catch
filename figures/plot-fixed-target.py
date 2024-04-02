# Licensed with the 3-clause BSD license.  See LICENSE for details.

import numpy as np
import matplotlib.pyplot as plt
from astropy.coordinates import Angle
import astropy.units as u

from sbsearch.visualization.fixed import plot_fixed
from catch import Catch, Config, model, FixedTarget, IntersectionType

ra: Angle = Angle("21 29 58.33", "hourangle")
dec: Angle = Angle("+12 10 01.2", "deg")
radius: Angle = Angle(30, "arcmin")
target: FixedTarget = FixedTarget.from_radec(ra, dec)


config: Config = Config.from_file("../catch-dev_aws.config")
catch: Catch
with Catch.with_config(config) as catch:
    print("Initialized catch.")

    catch.padding = radius.arcmin

    for intersection_type in IntersectionType:
        catch.intersection_type = intersection_type

        fig = plt.figure(clear=True)
        plot_fixed(catch, target)

        file_prefix: str = "query-{}-{}".format(
            str(target).strip(")").translate(str.maketrans(" (", "__")),
            intersection_type.name,
        )
        fig.savefig(f"{file_prefix}-narrow.png", dpi=200)

        ax = plt.gca()
        plt.setp(
            ax,
            xlim=np.array(ax.get_xlim()) * 5,
            ylim=np.array(ax.get_ylim()) * 5,
            aspect="equal",
            adjustable="datalim",
        )
        fig.savefig(f"{file_prefix}-wide.png", dpi=200)
