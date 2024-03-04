import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from astropy.time import Time
from spherical_geometry.polygon import SphericalPolygon, great_circle_arc, vector
from catch import Catch, Config, model
from sbsearch.core import line_to_segment_query_terms
from sbsearch.spatial import term_to_cell_vertices

target = '65P'
dates = ('2017-07-15', '2017-08-15')
# dates = ('2017-01-01', '2017-12-31')
# dates = ('2014-03-15', '2018-03-15')
view = (10, -110)  # elevation, azimuth for 3D plot
# config = Config(database='postgresql://@/catch_dev',
#                 log='/dev/null',
#                 debug=True)
config = Config.from_file("../catch-dev_aws.config")

file_suffix = (f'{target.lower().replace(" ", "").replace("/", "")}'
               f'-{dates[0].replace("-", "")}'
               f'-{dates[1].replace("-", "")}')

timestamps = []
timestamps.append(("Open catch...", Time.now()))
with Catch.with_config(config) as catch:
    timestamps.append(("Opened...", Time.now()))
    # get 65P query terms for Jul/Aug 2017
    comet = catch.get_designation(target)
    eph = comet.ephemeris(model.SkyMapper,
                          start=Time(dates[0]),
                          stop=Time(dates[1]))
    timestamps.append(("Got ephemeris", Time.now()))

    ra = np.array([e.ra for e in eph])
    dec = np.array([e.dec for e in eph])
    t = np.array([e.mjd for e in eph])
    query_terms = set(sum([
        terms for (terms, segment) in line_to_segment_query_terms(
            catch.indexer, np.radians(ra), np.radians(dec), t)], []))
    timestamps.append(("Got query terms", Time.now()))

    # convert query terms into cells
    query_cells = {
        term: np.degrees(term_to_cell_vertices(term.lstrip('$').encode()))
        for term in query_terms
    }
    timestamps.append(("Generated cell vertices", Time.now()))

    # get matching observations from database
    catch.source = 'skymapper'
    all_obs = catch.find_observations_by_ephemeris(eph, approximate=True)
    obs_by_ephemeris = catch.find_observations_by_ephemeris(eph)
    obs_terms = sum([obs.spatial_terms for obs in obs_by_ephemeris], [])

    timestamps.append(("Got observations", Time.now()))
    # detach data objects from database to make them persistent
    catch.db.session.expunge_all()

    timestamps.append(("Made observations persistent", Time.now()))

# ################################################################################


def quad_to_poly(ra, dec, **kwargs):
    p = SphericalPolygon.from_radec(ra, dec, degrees=True)

    points = p.polygons[0]._points
    _ra, _dec = [], []
    for A, B in zip(points[0:-1], points[1:]):
        length = great_circle_arc.length(A, B, degrees=True)
        if not np.isfinite(length):
            length = 2
        interpolated = great_circle_arc.interpolate(A, B, length * 4)
        lon, lat = vector.vector_to_lonlat(
            interpolated[:, 0], interpolated[:, 1], interpolated[:, 2],
            degrees=True)
        for lon0, lat0, lon1, lat1 in zip(lon[0:-1], lat[0:-1], lon[1:], lat[1:]):
            _ra.append(lon0)
            _dec.append(lat0)

    _ra.append(lon1)
    _dec.append(lat1)
    return PolyCollection([np.c_[_ra, _dec]], **kwargs)


def obs_to_poly(obs, **kwargs):
    ra, dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    ra = np.r_[ra, ra[0]]
    dec = np.r_[dec, dec[0]]
    return quad_to_poly(ra, dec, **kwargs)


def cell_to_poly(radec, **kwargs):
    ra = np.r_[radec[0], radec[0, 0]]
    ra[ra < 0] += 360
    dec = np.r_[radec[1], radec[1, 0]]
    return quad_to_poly(ra, dec, **kwargs)


def annotate(ax, text, x, y):
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    yt = (y
          + (-1 if y < np.mean(ylim) else 1)
          * 0.05 * np.ptp(ylim))
    if yt > max(ylim) - 0.1 * np.ptp(ylim):
        yt = y - 0.1 * np.ptp(ylim)
    if yt < min(ylim) + 0.1 * np.ptp(ylim):
        yt = y + 0.1 * np.ptp(ylim)

    if xlim[0] > xlim[1]:
        ha = 'left' if x > np.mean(xlim) else 'right'
    else:
        ha = 'right' if x > np.mean(xlim) else 'left'
    if ylim[0] > ylim[1]:
        va = 'bottom' if y < np.mean(ylim) else 'top'
    else:
        va = 'top' if y < np.mean(ylim) else 'bottom'
    ax.annotate(text, (x, y), (x, yt),
                ha=ha, va=va,
                arrowprops={'arrowstyle': '-',
                            'shrinkB': 4}
                )

# ################################################################################


fig = plt.figure(1)
fig.clear()
ax = fig.add_subplot()

ephemeris_style = dict(lw=1, ls='--', zorder=99, color='k')
all_obs_style = dict(lw=0.5, zorder=-99, color='tab:blue', fc='none')
matched_obs_style = dict(lw=2, zorder=100, color='tab:red', fc='none')

all_obs_line_style = {k: v for (k, v) in all_obs_style.items()
                      if k != 'fc'}
matched_obs_line_style = {k: v for (k, v) in matched_obs_style.items()
                          if k != 'fc'}

handles = []
handles.extend(ax.plot(ra, dec, label=target, **ephemeris_style))

poly = None
for obs in all_obs:
    poly = obs_to_poly(obs, **all_obs_style)
    ax.add_artist(poly)

if poly:
    poly.set_label('Matched by approximate search')
    handles.append(poly)

poly = None
for obs in obs_by_ephemeris:
    poly = obs_to_poly(obs, **matched_obs_style)
    ax.add_artist(poly)

if poly:
    poly.set_label('Matched by intersection')
    handles.append(poly)

plt.setp(ax, xlim=ax.get_xlim()[::-1], xlabel='RA (deg)', ylabel='Dec (deg)',
         aspect=1, adjustable='datalim')
ax.minorticks_on()
plt.legend(handles=handles)
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], ra[0], dec[0])
annotate(ax, dates[1], ra[-1], dec[-1])

plt.savefig(f'query-cells-ra-dec-{file_suffix}.png', dpi=200)
ra_lim = ax.get_xlim()
dec_lim = ax.get_ylim()


# ################################################################################

fig = plt.figure(2)
fig.clear()
ax = fig.add_subplot()

handles = []
handles.extend(ax.plot(ra, t, label=target, **ephemeris_style))

for obs in all_obs:
    _ra, _dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    h = plt.plot((_ra.min(), _ra.max()), [
                 obs.mjd_start] * 2, **all_obs_line_style)

h[0].set_label('Matched by approximate search')
handles.append(h[0])

for obs in obs_by_ephemeris:
    _ra, _dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    h = plt.plot((_ra.min(), _ra.max()), [obs.mjd_start] * 2,
                 **matched_obs_line_style)

h[0].set_label('Matched by intersection')
handles.append(h[0])

plt.setp(ax, xlim=ra_lim, xlabel='RA (deg)', ylabel='Date (MJD)')
ax.minorticks_on()
plt.legend(handles=handles, loc='upper right')
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], ra[0], t[0])
annotate(ax, dates[1], ra[-1], t[-1])

plt.savefig(f'query-cells-ra-time-{file_suffix}.png', dpi=200)

# ################################################################################

fig = plt.figure(3)
fig.clear()
ax = fig.add_subplot()

handles = []
handles.extend(ax.plot(t, dec, label=target, **ephemeris_style))

for obs in all_obs:
    _ra, _dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    h = plt.plot([obs.mjd_start] * 2,
                 (_dec.min(), _dec.max()), **all_obs_line_style)

h[0].set_label('Matched by approximate search')
handles.append(h[0])

for obs in obs_by_ephemeris:
    _ra, _dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    h = plt.plot([obs.mjd_start] * 2, (_dec.min(), _dec.max()),
                 label=obs.product_id, **matched_obs_line_style)

h[0].set_label('Matched by intersection')
handles.append(h[0])

plt.setp(ax, ylim=dec_lim, ylabel='Dec (deg)', xlabel='Date (MJD)')
ax.minorticks_on()
plt.legend(handles=handles, loc='upper right')
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], t[0], dec[0])
annotate(ax, dates[1], t[-1], dec[-1])

plt.savefig(f'query-cells-dec-time-{file_suffix}.png', dpi=200)

# ################################################################################

fig = plt.figure(4)
fig.clear()
ax = fig.add_subplot(projection='3d')

handles = []
handles.extend(ax.plot(ra, dec, t, label=target, **ephemeris_style))

for obs in all_obs:
    poly = obs_to_poly(obs, lw=all_obs_style['lw'],
                       edgecolors=[all_obs_style['color']],
                       facecolors=['none'])
    ax.add_collection3d(poly, zs=[obs.mjd_start])

for obs in obs_by_ephemeris:
    poly = obs_to_poly(obs, lw=matched_obs_style['lw'],
                       edgecolors=[matched_obs_style['color']],
                       facecolors=['none'])
    ax.add_collection3d(poly, zs=[obs.mjd_start])

ax.view_init(*view)
plt.setp(ax, xlim=ra_lim, xlabel='RA (deg)',
         ylim=dec_lim, ylabel='Dec (deg)')
plt.tight_layout()
plt.savefig(f'query-cells-ra-dec-time-{file_suffix}.png', dpi=200)


# ################################################################################

fig = plt.figure(5)
fig.clear()
ax = fig.add_subplot()

handles = []
handles.extend(ax.plot(ra, dec, label=target, **ephemeris_style))

for obs in obs_by_ephemeris:
    poly = obs_to_poly(obs, label='Matched by intersection',
                       facecolor='none', **matched_obs_style)
    ax.add_artist(poly)
    handles.append(poly)

obs_terms = sum([obs.spatial_terms for obs in obs_by_ephemeris], [])
matched = 0
for term, cell in query_cells.items():
    if term in obs_terms:
        fc = 'tab:pink'
        alpha = 0.5
        label = 'Matched S2 cell'
        matched += 1
    else:
        fc = 'none'
        alpha = 1
        label = None

    poly = cell_to_poly(cell, color='tab:brown', fc=fc, lw=0.75, alpha=alpha,
                        label=label)
    ax.add_artist(poly)

    if label is not None and matched == 1:
        handles.append(poly)

plt.setp(ax, xlim=ax.get_xlim()[::-1], xlabel='RA (deg)', ylabel='Dec (deg)',
         aspect=1, adjustable='datalim')
ax.minorticks_on()
ax.legend(handles=handles, loc='lower right')
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], ra[0], dec[0])
annotate(ax, dates[1], ra[-1], dec[-1])

plt.savefig(f'query-cells-{file_suffix}.png', dpi=200)

timestamps.append(("Plots generated", Time.now()))

t0 = timestamps[0][1]
for timestamp in timestamps:
    print(timestamp[0], timestamp[1].iso, (timestamp[1] - t0).jd * 86400)
