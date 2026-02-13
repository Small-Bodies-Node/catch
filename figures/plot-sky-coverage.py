r"""
Every observation must be checked against S2 cells distributed across the entire
sky.  Because of this, the benefits of working with the database are lost.
Instead, download a copy of the observation database (or subsets):

    \copy observation (source,mjd_start,fov) to observations.csv with (FORMAT CSV, HEADER);

Examples:

    python3 plot-sky-coverage.py --source=skymapper
    python3 plot-sky-coverage.py --source=neat_palomar_tricam
    python3 plot-sky-coverage.py --source=neat_maui_geodss

    for source in skymapper_dr4 neat_palomar_tricam neat_maui_geodss catalina_bigelow catalina_lemmon spacewatch ps1dr2 loneos; do
        python3 plot-sky-coverage.py  --source=$source
    done

"""

import os
import sys
import shlex
import argparse
import logging
from collections import Counter

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection

from astropy.io import ascii
from astropy.table import Table
from astropy.time import Time
from astropy.coordinates import Angle
import astropy.units as u
import pywraps2 as s2

from sbsearch.core import polygon_string_to_arrays
from sbsearch.spatial import term_to_cell_vertices
from sbsearch.logging import setup_logger, ProgressTriangle
from catch.model import Observation


def radec_to_fov(ra, dec):
    values = []
    _ra = (ra + 360) % 360
    for i in range(len(ra)):
        values.append(f"{_ra[i]:.6f}:{dec[i]:.6f}")
    return ",".join(values)


def fields_of_view(fn, source=None):
    with open(fn, "r") as inf:
        inf.readline()  # skip the required header
        count = 0
        for line in inf:
            if (source is not None) and not line.startswith(source):
                continue

            ra, dec = polygon_string_to_arrays(
                line[line.index(",") + 3 : -2]  # noqa: E203
            )
            ra = Angle(ra, "rad").wrap_at(180 * u.deg).rad
            vertices = []
            for i in range(4):
                vertices.append(s2.S2LatLng.FromRadians(dec[i], ra[i]).ToPoint())

            loop = s2.S2Loop(vertices)
            loop.Normalize()
            poly = s2.S2Polygon(loop)
            yield poly

            count += 1
            if count > 100:
                return


def nights(fn, source=None):
    with open(fn, "r") as inf:
        inf.readline()  # skip the required header
        mjd = []
        for line in inf:
            start = line.index(",") + 1
            mjd.append(int(line[start : line.index(".", start)]))
    count = Counter(mjd)
    return count


def cell_sky_coverage(fovs, level):
    counter = Counter()
    indexer = s2.S2RegionTermIndexer()
    indexer.set_fixed_level(level)

    tri = ProgressTriangle(1, logger)
    for region in fovs:
        terms = indexer.GetIndexTerms(region, "")
        counter.update(terms)
        tri.update()

    cells = []
    count = []
    fov = []
    for cell, n in counter.items():
        cells.append(cell)
        count.append(n)
        ra, dec = np.degrees(term_to_cell_vertices(cell))
        fov.append(radec_to_fov(ra, dec))

    return np.array(cells), np.array(count), np.array(fov)


def get_polygons(fov):
    ra, dec = polygon_string_to_arrays(fov)
    ra = Angle(ra, "rad").wrap_at(180 * u.deg).rad

    # split polygons that cross 0/360
    if ra.ptp() > np.pi / 2:
        _ra = [x if x < 0 else -np.pi for x in ra]
        yield np.array((_ra, dec)).transpose()
        _ra = [x if x > 0 else np.pi for x in ra]
        yield np.array((_ra, dec)).transpose()
    else:
        yield np.array((ra, dec)).transpose()


def plot(fov_count, fov, mjd_count, mjd, source_name, date):
    plt.style.use("dark_background")

    fig = plt.figure(clear=True, figsize=(5 * 16 / 9, 5))
    # ax = plt.subplot(projection="mollweide")
    ax = fig.add_axes((0, 0.3, 0.95, 0.65), projection="mollweide")
    plt.title(source_name)
    plt.grid(True, linewidth=0.5)

    fov_count = np.ma.MaskedArray(fov_count, mask=fov_count == 0)
    norm = mpl.colors.LogNorm(vmin=1, vmax=fov_count.max())
    polygons = []
    for _fov, scale in zip(fov, norm(fov_count)):
        for coords in get_polygons(_fov):
            polygons.append(
                Polygon(
                    coords,
                    edgecolor="none",
                    facecolor=plt.cm.magma(scale),
                )
            )

    ax.add_artist(PatchCollection(polygons, match_original=True))

    cb = plt.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap="magma"),
        ax=ax,
        location="left",
        orientation="vertical",
        aspect=15,
    )
    cb.set_label("Count")
    cb.ax.set_yscale("log")

    ax.text(
        0.98,
        0.98,
        date,
        transform=fig.transFigure,
        fontsize=7,
        ha="right",
        va="top",
    )

    hax = fig.add_axes((0.1, 0.1, 0.85, 0.1))
    year0 = int(Time(mjd.min(), format="mjd").iso[:4]) + 1
    year = (mjd - Time(f"{year0}-01-01").mjd) / 365.25 + year0
    hax.hist(year, weights=mjd_count, bins=300, color=plt.get_cmap("magma")(0.5))
    hax.spines[["right", "top"]].set_visible(False)
    hax.set_yscale("log")
    hax.set_xlabel("Year")
    hax.set_ylabel("Count")


def get_source_name(source):
    if source == "observation":
        return "All data sources"
    else:
        for Source in Observation.__subclasses__():
            if source == Source.__tablename__:
                return Source.__data_source_name__

    raise ValueError(source)


def observations_file_date():
    """The date of the observations file used for plot annotation."""

    try:
        stat = os.stat("observations.csv")
        return Time(stat.st_ctime, format="unix").iso[:10]
    except FileNotFoundError:
        return ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="CATCH conf        default=" ",uration file.")
    parser.add_argument(
        "--source",
        help="limit analysis to this data source (default: show all data)",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=6,
        help="S2 cell level, default is 6 for 1.3 deg resolution",
    )
    parser.add_argument(
        "-o", default=None, help="output file name prefix, default based on --source"
    )
    parser.add_argument(
        "--date",
        default=observations_file_date(),
        help=(
            "annotate the plot with this date (default is to use "
            "the date of the observations.csv file)"
        ),
    )
    parser.add_argument("--format", default="png", help="plot file format")
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument(
        "-f",
        dest="force",
        action="store_true",
        help="ignore previous saved file and reprocess",
    )
    parser.add_argument("-v", action="store_true", help="show debug messages")
    args = parser.parse_args()

    logger = logging.getLogger("Plot Sky Coverage")
    if len(logger.handlers) == 0:
        logger = setup_logger(
            "plot-sky-coverage.log",
            "Plot Sky Coverage",
            logging.DEBUG if args.v else logging.INFO,
        )

    logger.info("%s", " ".join([shlex.quote(s) for s in sys.argv]))

    prefix = args.source if args.o is None else args.o
    prefix = "all" if prefix is None else prefix
    table_fn = f"{prefix}-level{args.level}.csv"

    source_name = None if args.source is None else get_source_name(args.source)
    if os.path.exists(table_fn) and not args.force:
        tab = ascii.read(table_fn)
        cells = tab["cell"].data
        fov_count = tab["count"].data
        fov = tab["fov"].data
    else:
        fovs = fields_of_view("observations.csv", args.source)
        cells, fov_count, fov = cell_sky_coverage(fovs, args.level)

        tab = Table((cells, fov_count, fov), names=("cell", "count", "fov"))
        tab.sort("cell")
        tab.write(table_fn, overwrite=True)

    table_fn = f"{prefix}-nights.csv"
    if os.path.exists(table_fn) and not args.force:
        tab = ascii.read(table_fn)
    else:
        n = nights("observations.csv", args.source)
        tab = Table(rows=n.items(), names=("mjd", "count"))
        tab.sort("mjd")
        tab.write(table_fn, overwrite=True)

    mjd = tab["mjd"].data
    mjd_count = tab["count"].data

    plot(fov_count, fov, mjd_count, mjd, source_name, args.date)
    plt.savefig(f"{prefix}-level{args.level}.{args.format}", dpi=args.dpi)
