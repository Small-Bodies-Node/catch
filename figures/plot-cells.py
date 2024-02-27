import argparse
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from spherical_geometry.polygon import (
    SphericalPolygon,
    great_circle_arc,
    vector,
)
from catch import Catch, Config, model
from sbsearch.core import line_to_segment_query_terms
from sbsearch.spatial import term_to_cell_vertices

parser = argparse.ArgumentParser()
parser.add_argument("observation_id", type=int)
parser.add_argument("--term", "-t", action="append", dest="terms",
                    help="also plot these cell terms")
parser.add_argument("--format", default="png", help="figure format, e.g., png")
args = parser.parse_args()

config = Config(
    database="postgresql://@/catch", log="/dev/null", debug=True
)


def quad_to_poly(ra, dec, **kwargs):
    p = SphericalPolygon.from_radec(ra, dec, degrees=True)

    points = p.polygons[0]._points
    _ra, _dec = [], []
    for A, B in zip(points[0:-1], points[1:]):
        length = great_circle_arc.length(A, B, degrees=True)
        if not np.isfinite(length):
            length = 2
        interpolated = great_circle_arc.interpolate(A, B, length * 16)
        lon, lat = vector.vector_to_lonlat(
            interpolated[:, 0], interpolated[:, 1], interpolated[:, 2],
            degrees=True)
        for lon0, lat0, lon1, lat1 in zip(lon[0:-1], lat[0:-1], lon[1:], lat[1:]):
            _ra.append(lon0)
            _dec.append(lat0)

    _ra.append(lon1)
    _dec.append(lat1)

    return _ra, _dec, PolyCollection([np.c_[_ra, _dec]], **kwargs)


def fov_to_poly(fov, **kwargs):
    ra, dec = np.array([c.split(':') for c in fov.split(',')], float).T
    ra = np.r_[ra, ra[0]]
    dec = np.r_[dec, dec[0]]
    return quad_to_poly(ra, dec, **kwargs)


def cell_to_poly(radec, **kwargs):
    ra = np.r_[radec[0], radec[0, 0]]
    ra[ra < 0] += 360
    dec = np.r_[radec[1], radec[1, 0]]
    return quad_to_poly(ra, dec, **kwargs)


with Catch.with_config(config) as catch:
    spatial_terms, fov = (
        catch.db.session.query(model.Observation.spatial_terms,
                               model.Observation.fov)
        .filter(model.Observation.observation_id == args.observation_id)
        .one()
    )
    cells = {term: np.degrees(term_to_cell_vertices(term.lstrip("$").encode()))
             for term in spatial_terms}

fig = plt.figure(1, (8, 4))
fig.clear()
lax = fig.add_subplot(121)
rax = fig.add_subplot(122)

for ax in (lax, rax):
    poly = fov_to_poly(fov, color='k', fc='none', lw=0.75, zorder=99)[2]
    ax.add_artist(poly)

labeled = []
covering_terms = [k[1:] for k in cells.keys() if k.startswith('$')]
ancestor_terms = [k for k in cells.keys() if k not in covering_terms]
for term, cell in cells.items():
    if term.startswith('$'):
        fc = 'tab:red'
        alpha = 0.5
        if '$' not in labeled:
            label = 'Covering cells'
            labeled.append('$')
        else:
            label = ''
        ax = lax
    elif term in ancestor_terms:
        fc = 'tab:blue'
        alpha = 0.5
        if '' not in labeled:
            label = 'Ancestor cells'
            labeled.append('')
        else:
            label = ''
        ax = rax
    else:
        # the indexer provides ancestor terms for each covering term, but don't
        # plot them
        continue

    ra, dec, poly = cell_to_poly(cell, color='k', fc=fc, lw=0.75,
                                 alpha=alpha, label=label)
    ax.add_artist(poly)

if args.terms is not None:
    for term in args.terms:
        cell = np.degrees(term_to_cell_vertices(term.lstrip("$").encode()))
        for ax in (lax, rax):
            ra, dec, poly = cell_to_poly(
                cell, color='tab:red', fc='none', lw=0.75)
            ax.add_artist(poly)

ra = np.r_[[ra for (ra, dec) in cells.values()]] % 360
dec = np.r_[[dec for (ra, dec) in cells.values()]]
xlim = np.array((ra.max(), ra.min()))
ylim = np.array((dec.min(), dec.max()))

# adjust to make a ~square image
cdec = np.cos(np.radians(ylim.mean()))
size = np.max([xlim.ptp() * cdec, ylim.ptp()]) * 1.1
xlim = xlim.mean() + np.array((1, -1)) / 2 * size / cdec
ylim = ylim.mean() + np.array((-1, 1)) / 2 * size

plt.setp((lax, rax), xlim=xlim, ylim=ylim, xlabel='RA (deg)', ylabel='Dec (deg)',
         aspect=1 / cdec)
rax.set_yticklabels([])
rax.set_ylabel(None)

for ax in (lax, rax):
    ax.minorticks_on()
    ax.legend()

plt.tight_layout(pad=1)


plt.savefig(f's2cells-obsid-{args.observation_id}.{args.format}')
