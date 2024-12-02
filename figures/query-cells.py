import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection, LineCollection
from matplotlib.legend_handler import HandlerPatch
from astropy.time import Time
from astropy.wcs import WCS
from catch import Catch, Config, model
from sbsearch.core import line_to_segment_query_terms
from sbsearch.spatial import term_to_cell_vertices
from sbsearch.visualization import plot_terms, plot_observations

target = "65P"
dates = ("2023-05-01", "2024-05-01")
projection = "TAN"
view = (10, -110)  # elevation, azimuth for 3D plot
config = Config.from_file("../../aws-catch_prod_v3.config")

file_suffix = (
    f'{target.lower().replace(" ", "").replace("/", "")}'
    f'-{dates[0].replace("-", "")}'
    f'-{dates[1].replace("-", "")}'
)
data_file = f"query-cells-{file_suffix}.json"


def catch_target():
    timestamps = []
    timestamps.append(["Open catch...", Time.now().iso])
    with Catch.with_config(config) as catch:
        timestamps.append(["Opened...", Time.now().iso])
        # get 65P query terms for Jul/Aug 2017
        comet = catch.get_designation(target)
        eph = comet.ephemeris(
            model.CatalinaBigelow, start=Time(dates[0]), stop=Time(dates[1])
        )
        timestamps.append(["Got ephemeris", Time.now().iso])

        ra = np.array([e.ra for e in eph])
        dec = np.array([e.dec for e in eph])
        mjd = np.array([e.mjd for e in eph])
        query_terms = set(
            sum(
                [
                    terms
                    for (terms, segment) in line_to_segment_query_terms(
                        catch.indexer, np.radians(ra), np.radians(dec), mjd
                    )
                ],
                (),
            )
        )
        timestamps.append(["Got query terms", Time.now().iso])

        # convert query terms into cells
        query_cells = {
            term: np.degrees(term_to_cell_vertices(term.lstrip("$")))
            for term in query_terms
        }
        timestamps.append(["Generated cell vertices", Time.now().iso])

        # get matching observations from database
        catch.source = "catalina_bigelow"
        all_obs = catch.find_observations_by_ephemeris(eph, approximate=True)
        obs_by_ephemeris = catch.find_observations_by_ephemeris(eph)
        obs_terms = list(set(sum([obs.spatial_terms for obs in obs_by_ephemeris], [])))

        timestamps.append(["Got observations", Time.now().iso])
        # detach data objects from database to make them persistent
        catch.db.session.expunge_all()

        timestamps.append(["Made observations persistent", Time.now().iso])

        return (
            timestamps,
            ra,
            dec,
            mjd,
            query_terms,
            all_obs,
            obs_by_ephemeris,
            obs_terms,
        )


timestamps, ra, dec, mjd, query_terms, all_obs, obs_by_ephemeris, obs_terms = (
    catch_target()
)


def annotate(ax, text, x, y, wcs=None):
    if wcs is None:
        xp = x
        yp = y
    else:
        xp, yp = wcs.all_world2pix([[x, y]], 0).squeeze()
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()

    yt = yp + (-1 if y < np.mean(ylim) else 1) * 0.05 * np.ptp(ylim)
    if yt > max(ylim) - 0.1 * np.ptp(ylim):
        yt = yp - 0.1 * np.ptp(ylim)
    if yt < min(ylim) + 0.1 * np.ptp(ylim):
        yt = yp + 0.1 * np.ptp(ylim)

    if xlim[0] > xlim[1]:
        ha = "left" if xp > np.mean(xlim) else "right"
    else:
        ha = "right" if xp > np.mean(xlim) else "left"
    if ylim[0] > ylim[1]:
        va = "bottom" if yp < np.mean(ylim) else "top"
    else:
        va = "top" if yp < np.mean(ylim) else "bottom"

    ax.annotate(
        text,
        (xp, yp),
        (xp, yt),
        ha=ha,
        va=va,
        arrowprops={"arrowstyle": "-", "shrinkB": 4},
        zorder=1000,
    )


class PatchCollectionHandler(HandlerPatch):
    # adapted from Google Gemini
    def create_artists(
        self, legend, orig_handle, xdescent, ydescent, width, height, fontsize, trans
    ):
        fc = (
            orig_handle.get_facecolor()
            if len(orig_handle.get_facecolor()) > 0
            else "none"
        )
        ec = (
            orig_handle.get_edgecolor()
            if len(orig_handle.get_edgecolor()) > 0
            else "none"
        )
        p = Rectangle(
            (0, 0),
            width,
            height,
            facecolor=fc,
            edgecolor=ec,
            alpha=orig_handle.get_alpha(),
        )
        return [p]


def get_radec(obs):
    return np.array([c.split(":") for c in obs.fov.split(",")], float).T


def adjust_limits(ax, x, y):
    xlim = ax.get_xlim()
    xptp = np.ptp(x)
    xrange = np.array((np.min(x) - 0.1 * xptp, np.max(x) + 0.1 * xptp))
    ax.set_xlim(min(np.min(xrange), xlim[0]), max(np.max(xrange), xlim[1]))

    ylim = ax.get_ylim()
    yptp = np.ptp(y)
    yrange = np.array((np.min(y) - 0.1 * yptp, np.max(y) + 0.1 * yptp))
    ax.set_ylim(min(np.min(yrange), ylim[0]), max(np.max(yrange), ylim[1]))


# ################################################################################


fig = plt.figure(1)
fig.clear()

# WCS for plot projections
wcs = WCS(naxis=2)
wcs.wcs.crpix = [0, 0]
wcs.wcs.cdelt = np.array([-1, 1])
wcs.wcs.crval = [np.mean((np.min(ra), np.max(ra))), np.mean((np.min(dec), np.max(dec)))]
wcs.wcs.ctype = [f"RA---{projection}", f"DEC--{projection}"]
wcs.wcs.radesys = "ICRS"
ax = plt.axes(projection=wcs)
transform = {"transform": ax.get_transform("world")}

ephemeris_style = dict(lw=1, ls="--", zorder=99, color="k")
all_obs_style = dict(
    lw=0.5,
    zorder=-99,
    color="tab:blue",
    fc="none",
    label="Observations matched by fuzzy search",
)
matched_obs_style = dict(
    lw=2,
    zorder=100,
    color="tab:red",
    fc="none",
    label="Observations matched by intersection",
)

ax.plot(ra, dec, label=target, **transform, **ephemeris_style)
plot_observations(ax, all_obs, **all_obs_style)
plot_observations(ax, obs_by_ephemeris, **matched_obs_style)

plt.setp(
    ax,
    xlabel="Right Ascension",
    ylabel="Declination",
    aspect=1,
    adjustable="datalim",
)

x, y = wcs.all_world2pix(
    np.concatenate([get_radec(obs)[0] for obs in all_obs]),
    np.concatenate([get_radec(obs)[1] for obs in all_obs]),
    0,
)
adjust_limits(ax, x, y)
ax.minorticks_on()
ax.grid()
plt.legend(handler_map={PatchCollection: PatchCollectionHandler()})
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], ra[0], dec[0], wcs)
annotate(ax, dates[1], ra[-1], dec[-1], wcs)

plt.savefig(f"query-cells-ra-dec-{file_suffix}.png", dpi=300)

# ################################################################################

fig = plt.figure(2)
fig.clear()
ax = fig.add_subplot()

ax.plot(ra, mjd, label=target, **ephemeris_style)

lines = []
for obs in all_obs:
    _ra = get_radec(obs)[0]
    lines.append(np.c_[(_ra.min(), _ra.max()), [obs.mjd_start] * 2])
    adjust_limits(ax, _ra, [obs.mjd_start])
ax.add_collection(LineCollection(lines, **all_obs_style))

lines = []
for obs in obs_by_ephemeris:
    _ra = get_radec(obs)[0]
    lines.append(np.c_[(_ra.min(), _ra.max()), [obs.mjd_start] * 2])
ax.add_collection(LineCollection(lines, **matched_obs_style))

plt.setp(ax, xlim=ax.get_xlim()[::-1], xlabel="RA (deg)", ylabel="Date (MJD)")
ax.minorticks_on()
plt.legend(loc="upper right")
plt.tight_layout(pad=0.2)

ra_lim = ax.get_xlim()
annotate(ax, dates[0], ra[0], mjd[0])
annotate(ax, dates[1], ra[-1], mjd[-1])

plt.savefig(f"query-cells-ra-time-{file_suffix}.png", dpi=300)

# ################################################################################

fig = plt.figure(3)
fig.clear()
ax = fig.add_subplot()

ax.plot(mjd, dec, label=target, **ephemeris_style)

lines = []
for obs in all_obs:
    _dec = get_radec(obs)[1]
    lines.append(np.c_[[obs.mjd_start] * 2, (_dec.min(), _dec.max())])
    adjust_limits(ax, [obs.mjd_start], _dec)
ax.add_collection(LineCollection(lines, **all_obs_style))

lines = []
for obs in obs_by_ephemeris:
    _dec = get_radec(obs)[1]
    lines.append(np.c_[[obs.mjd_start] * 2, (_dec.min(), _dec.max())])
ax.add_collection(LineCollection(lines, **matched_obs_style))

plt.setp(ax, ylabel="Dec (deg)", xlabel="Date (MJD)")
ax.minorticks_on()
plt.legend(loc="upper right")
plt.tight_layout(pad=0.2)

dec_lim = ax.get_ylim()
annotate(ax, dates[0], mjd[0], dec[0])
annotate(ax, dates[1], mjd[-1], dec[-1])

plt.savefig(f"query-cells-dec-time-{file_suffix}.png", dpi=300)

# ################################################################################

# fig = plt.figure(4)
# fig.clear()
# ax = fig.add_subplot(projection="3d")

# # ax.plot(ra, dec, mjd, label=target, **ephemeris_style)

# # polygons = []
# # z = []
# # for obs in all_obs:
# #     coords = np.array(polygon_string_to_arrays(obs.fov)).T
# #     polygons.append([(r, d, obs.mjd_start) for r, d in coords])
# polygons = [
#     [[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0]],  # Bottom face
#     [[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 1, 1]],  # Top face
#     [[0, 0, 0], [1, 0, 0], [1, 0, 1], [0, 0, 1]],  # Front face
#     [[1, 0, 0], [1, 1, 0], [1, 1, 1], [1, 0, 1]],  # Right face
#     [[0, 1, 0], [1, 1, 0], [1, 1, 1], [0, 1, 1]],  # Back face
#     [[0, 0, 0], [0, 1, 0], [0, 1, 1], [0, 0, 1]],  # Left face
# ]
# ax.add_collection3d(Poly3DCollection(polygons, **all_obs_style))

# polygons = []
# z = []
# for obs in obs_by_ephemeris:
#     polygons.append(observation_to_spherical_polygon(obs))
#     z.append(obs.mjd_start)
# ax.add_collection3d(PatchCollection(polygons, **matched_obs_style), zs=z)

# ax.view_init(*view)
# plt.setp(ax, xlim=ra_lim, xlabel="RA (deg)", ylim=dec_lim, ylabel="Dec (deg)")
# plt.tight_layout()
# plt.savefig(f"query-cells-ra-dec-time-{file_suffix}.png", dpi=300)

# ################################################################################

fig = plt.figure(5)
fig.clear()
ax = plt.axes(projection=wcs)
transform = {"transform": ax.get_transform("world")}

ax.plot(ra, dec, label=target, **transform, **ephemeris_style)

plot_observations(ax, obs_by_ephemeris, **matched_obs_style)

query_terms_style = dict(
    color="tab:brown",
    fc="none",
    lw=0.75,
    alpha=1,
    label="S2 query cells",
)
plot_terms(ax, query_terms, **query_terms_style)

obs_terms = set(sum([obs.spatial_terms for obs in obs_by_ephemeris], []))
matched_terms = query_terms & obs_terms
matched_terms_style = dict(
    fc="tab:pink",
    alpha=0.5,
    label="Matched S2 cells",
)
plot_terms(ax, matched_terms, **matched_terms_style)

plt.setp(
    ax,
    xlabel="Right Ascension",
    ylabel="Declination",
    aspect=1,
    adjustable="datalim",
)

x, y = wcs.all_world2pix(
    np.concatenate([get_radec(obs)[0] for obs in obs_by_ephemeris]),
    np.concatenate([get_radec(obs)[1] for obs in obs_by_ephemeris]),
    0,
)
adjust_limits(ax, x, y)
ax.minorticks_on()
ax.grid()
ax.legend(handler_map={PatchCollection: PatchCollectionHandler()})
plt.tight_layout(pad=0.2)

annotate(ax, dates[0], ra[0], dec[0], wcs)
annotate(ax, dates[1], ra[-1], dec[-1], wcs)

plt.savefig(f"query-cells-{file_suffix}.png", dpi=300)

timestamps.append(("Plots generated", Time.now()))

t0 = Time(timestamps[0][1])
for timestamp in timestamps:
    print(timestamp[0], timestamp[1], (Time(timestamp[1]) - t0).jd * 86400)
