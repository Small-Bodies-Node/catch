# Licensed with the 3-clause BSD license.  See LICENSE for details.

import argparse

# from time import monotonic
# from collections import defaultdict
# from typing import Dict, List, Set, Tuple
import numpy as np
import matplotlib.pyplot as plt

# from matplotlib.colors import TABLEAU_COLORS
# from matplotlib.collections import PatchCollection
# from matplotlib.patches import Rectangle

from astropy.coordinates import Angle

# from astropy.wcs import WCS
# from astropy.visualization.wcsaxes import SphericalCircle
import astropy.units as u

from sbsearch.visualization.fixed import plot_fixed
from catch import Catch, Config, model, FixedTarget, IntersectionType

# parser = argparse.ArgumentParser()
# parser.add_argument(
#     "ra",
#     "right ascension",
#     default="21 29 58.33",
# )
# parser.add_argument(
#     "dec",
#     "declination",
#     default="+12 10 01.2",
# )
# parser.add_argument(
#     "--unit",
#     default="hourangle,deg",
#     help="RA, Dec unit, may be a single string, or two separated"
#     " by a comma (default: hourangle,deg)",
# )
# parser.add_argument(
#     "--radius",
#     type=u.Quantity,
#     default=u.Quantity("30 arcmin"),
#     help="search this area around the target, default: 30 arcmin",
# )
# parser.add_argument(
#     "--intersection-type",
#     choices=[itype.name for itype in IntersectionType],
#     type=lambda itype: IntersectionType[itype],
#     help="type of observation field-of-view intersections allowed with the search area, default: ImageIntersectsArea",
# )

# parser.add_argument(
#     "--config",
#     help="use this configuration file",
# )
# parser.add_argument(
#     "--database",
#     help="use this database URI",
# )
# parser.add_argument(
#     "--log",
#     help="save log messages to this file",
# )

# parser.add_argument(
#     "--dpi",
#     type=int,
#     default=200,
#     help="figure DPI setting",
# )
# parser.add_argument(
#     "--format",
#     default="png",
#     help="figure format (file extension)",
# )

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

# ra: Angle = Angle("21 29 58.33", "hourangle")
# dec: Angle = Angle("+12 10 01.2", "deg")
# radius: Angle = Angle(30, "arcmin")
# target: FixedTarget = FixedTarget(SkyCoord(ra, dec))

# # WCS for plot projection
# wcs = WCS(naxis=2)
# wcs.wcs.crpix = [0, 0]
# wcs.wcs.cdelt = np.array([-1, 1]) / 3600
# wcs.wcs.crval = [target.ra.deg, target.dec.deg]
# wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
# wcs.wcs.radesys = "ICRS"

# config: Config = Config.from_file("../catch-dev_aws.config")

# file_suffix: str = ra.to_string() + dec.to_string(alwayssign=True)

# catch: Catch
# timestamps: Tuple[str, float] = []
# timestamps.append(("Initializing catch...", monotonic()))
# with Catch.with_config(config) as catch:
#     timestamps.append(("...done", monotonic()))

#     catch.source = model.Observation
#     catch.padding = radius.arcmin

#     # get query terms
#     query_terms: Set[str] = set(
#         catch.indexer.query_point(target.ra.rad, target.dec.rad)
#     )
#     timestamps.append(("Got query terms", monotonic()))

#     # get matching observations from database
#     observations: Dict[IntersectionType, List[model.Observation]] = defaultdict(list)
#     obs_terms: Dict[IntersectionType, Set[str]] = defaultdict(set)
#     for k in IntersectionType:
#         print(".", end="", flush=True)
#         catch.intersection_type = k
#         if k != IntersectionType.ImageContainsCenter:
#             catch.padding = radius.arcmin
#         observations[k] = catch.find_observations_intersecting_cap(target)
#         obs_terms[k] = set(sum([obs.spatial_terms for obs in observations[k]], []))

#     print()

#     timestamps.append(("Got observations", monotonic()))

#     # detach data objects from database to make them persistent
#     catch.db.session.expunge_all()
#     timestamps.append(("Made observations persistent", monotonic()))


# def plot_polys(ax, handles, items, func, style, label):
#     if len(items) > 0:
#         collection = PatchCollection(
#             [func(item) for item in items],
#             transform=ax.get_transform("icrs"),
#             **style,
#         )

#         ax.add_artist(collection)
#         handles.append(Rectangle((0, 0), 1, 1, label=label, **style))


# def plot(intersection_type):
#     fig, ax = plt.subplots(
#         num=intersection_type.value,
#         clear=True,
#         subplot_kw=dict(projection=wcs),
#     )

#     fixed_target_style = dict(
#         zorder=4, marker="*", color="y", edgecolor="k", linewidths=0.5, s=50
#     )
#     query_style = dict(lw=1, zorder=2, color="tab:brown", fc="none")
#     matched_cell_style = dict(zorder=1, color="none", fc="tab:pink", alpha=0.33)
#     matched_obs_style = dict(lw=0.75, zorder=0, color="tab:red", fc="none", alpha=0.33)

#     handles = []
#     ax.add_patch(
#         SphericalCircle(
#             target.coordinates,
#             radius=radius,
#             color="tab:blue",
#             lw=1,
#             fc=TABLEAU_COLORS["tab:blue"] + "55",
#             zorder=3,
#             transform=ax.get_transform("icrs"),
#         )
#     )
#     handles.append(
#         ax.scatter(
#             ra.deg,
#             dec.deg,
#             label=target,
#             transform=ax.get_transform("icrs"),
#             **fixed_target_style,
#         )
#     )

#     plot_polys(
#         ax,
#         handles,
#         query_terms,
#         term_to_spherical_polygon,
#         query_style,
#         "Query",
#     )

#     plot_polys(
#         ax,
#         handles,
#         query_terms.intersection(obs_terms[intersection_type]),
#         term_to_spherical_polygon,
#         matched_cell_style,
#         "Matched cells",
#     )

#     plot_polys(
#         ax,
#         handles,
#         observations[intersection_type],
#         observation_to_spherical_polygon,
#         matched_obs_style,
#         "Observations",
#     )

#     cdelt = wcs.proj_plane_pixel_scales()
#     narrow = max(radius * 6, 1 * u.deg)
#     narrow_xlim = (np.r_[-1, 1] * narrow / 2 / cdelt[0]).to_value("")
#     narrow_ylim = (np.r_[-1, 1] * narrow / 2 / cdelt[1]).to_value("")

#     plt.setp(
#         ax,
#         xlim=narrow_xlim,
#         ylim=narrow_ylim,
#         xlabel="RA",
#         ylabel="Dec",
#         aspect="equal",
#         adjustable="datalim",
#     )

#     for a in ax.coords:
#         a.display_minor_ticks(True)

#     ax.legend(handles=handles, loc="upper left", fontsize="medium")
#     fig.tight_layout(pad=0.5)
#     fig.savefig(
#         "query-{}-{}-narrow.png".format(
#             str(target).strip(")").translate(str.maketrans(" (", "__")),
#             intersection_type.name,
#         ),
#         dpi=200,
#     )

#     plt.setp(
#         ax,
#         xlim=np.array(ax.get_xlim()) * 5,
#         ylim=np.array(ax.get_ylim()) * 5,
#         aspect="equal",
#         adjustable="datalim",
#     )

#     fig.savefig(
#         "query-{}-{}-wide.png".format(
#             str(target).strip(")").translate(str.maketrans(" (", "__")),
#             intersection_type.name,
#         ),
#         dpi=200,
#     )


# for intersection_type in observations.keys():
#     plot(intersection_type)

# timestamps.append(("Plots generated", monotonic()))

# print()
# t0 = timestamps[0][1]
# last = t0
# for timestamp in timestamps:
#     print(f"{timestamp[0]:30s} {timestamp[1] - t0:.3f} {timestamp[1] - last:.3f}")
#     last = timestamp[1]
