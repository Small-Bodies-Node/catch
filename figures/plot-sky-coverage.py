import os
import argparse
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon

from astropy.io import ascii
from astropy.table import Table
import pywraps2 as s2

from sbsearch.spatial import term_to_cell_vertices
from sbsearch.logging import ProgressBar
from catch import Catch, Config
from catch.model import Observation

"""
Examples:
python3 plot-sky-coverage.py --config=../catch_prod-aws.config --source=skymapper
python3 plot-sky-coverage.py --config=../catch_prod-aws.config --source=neat_palomar_tricam
python3 plot-sky-coverage.py --config=../catch_prod-aws.config --source=neat_maui_geodss

for source in skymapper_dr4 neat_palomar_tricam neat_maui_geodss catalina_bigelow catalina_lemmon spacewatch ps1dr2 loneos; do
  echo python3 plot-sky-coverage.py --config=../catch-prod-aws.config --source=$source
done

"""


def radec_to_fov(ra, dec):
    values = []
    _ra = (ra + 360) % 360
    for i in range(len(ra)):
        values.append(f"{_ra[i]:.6f}:{dec[i]:.6f}")
    return ",".join(values)


def cell_sky_coverage(catch, level):
    terms = []
    count = []
    fov = []

    # first cell
    cell = s2.S2CellId.Begin(level)

    # break iteration at this cell
    end = s2.S2CellId.End(level)

    progress = ProgressBar(6 * 4**level, catch.logger, length=62)

    while True:
        term = cell.ToToken()

        terms.append(term)
        count.append(query_cell(catch, term))
        ra, dec = np.degrees(term_to_cell_vertices(term.lstrip("$")))
        fov.append(radec_to_fov(ra, dec))

        progress.update()
        cell = cell.next()

        if cell == end:
            break

    return np.array(terms), np.array(count), np.array(fov)


def query_cell(catch, term):
    query = catch.db.session.query(Observation)
    query = catch._filter_by_source(query)
    query = query.filter(catch.source.spatial_terms.overlap(["$" + term, term]))
    count = query.count()
    return count


def get_polygons(ra, dec, s):
    # splits polygons spanning the terminator into two
    if ra.ptp() > 1.5 * np.pi:
        _ra = [x if x < 0 else -np.pi for x in ra]
        yield Polygon(np.c_[_ra, dec], color=plt.cm.magma(s))
        _ra = [x if x > 0 else np.pi for x in ra]
        yield Polygon(np.c_[_ra, dec], color=plt.cm.magma(s))
    else:
        yield Polygon(np.c_[ra, dec], color=plt.cm.magma(s))


def plot(terms, count, source_name):
    plt.style.use("dark_background")

    fig = plt.figure(clear=True)
    ax = plt.subplot(projection="mollweide")
    plt.title(source_name)
    plt.grid(True)

    # determine color scale
    scale = np.log(count)
    empty = ~np.isfinite(scale)
    scale[empty] = np.nan
    if all(empty):
        print("nothing to plot")
        return

    scale = scale / scale[~empty].ptp()
    for term, s in zip(terms, scale):
        ra, dec = term_to_cell_vertices(term)
        for poly in get_polygons(ra, dec, s):
            ax.add_patch(poly)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="CATCH configuration file.")
    parser.add_argument(
        "--source",
        default="observation",
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
    parser.add_argument("--format", default="png", help="plot file format")
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument(
        "-f",
        dest="force",
        action="store_true",
        help="ignore previous saved file and reprocess",
    )
    args = parser.parse_args()

    prefix = args.source if args.o is None else args.o
    table_fn = f"{prefix}-level{args.level}.csv"

    config = Config.from_file(args.config)
    with Catch.with_config(config) as catch:
        catch.source = args.source
        source_name = catch.source.__data_source_name__
        if os.path.exists(table_fn) and not args.force:
            tab = ascii.read(table_fn)
            terms = tab["term"].data
            count = tab["count"].data
            fov = tab["fov"].data
        else:
            terms, count, fov = cell_sky_coverage(catch, args.level)

            tab = Table((terms, count, fov), names=("term", "count", "fov"))
            tab.write(table_fn, overwrite=True)
            tab.pprint()

    plot(terms, count, source_name)
    plt.savefig(f"{prefix}-level{args.level}.{args.format}", dpi=args.dpi)
