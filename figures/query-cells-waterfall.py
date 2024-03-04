from time import monotonic
timestamps = [("Python imports", monotonic())]
from sbsearch.core import line_to_segment_query_terms
from catch import Catch, Config, model
from astropy.time import Time
import matplotlib.pyplot as plt
import numpy as np


target = '65P'
# dates = ('2017-07-15', '2017-08-15')
# dates = ('2017-01-01', '2017-12-31')
dates = ('2014-03-15', '2018-03-15')
view = (10, -110)  # elevation, azimuth for 3D plot
# config = Config(database='postgresql://@/catch_dev',
#                 log='/dev/null',
#                 debug=True)
config = Config.from_file("../catch-dev_aws.config",
                          debug=True)

file_suffix = (f'{target.lower().replace(" ", "").replace("/", "")}'
               f'-{dates[0].replace("-", "")}'
               f'-{dates[1].replace("-", "")}')

timestamps.append(("CATCH\ninitialization", monotonic()))
with Catch.with_config(config) as catch:
    timestamps.append(("Ephemeris from JPL", monotonic()))
    # get 65P query terms for Jul/Aug 2017
    comet = catch.get_designation(target)
    eph = comet.ephemeris(model.SkyMapper,
                          start=Time(dates[0]),
                          stop=Time(dates[1]))

    # timestamps.append(("Generate\nS2 index terms", monotonic()))
    ra = np.array([e.ra for e in eph])
    dec = np.array([e.dec for e in eph])
    t = np.array([e.mjd for e in eph])
    # query = catch.indexer_query_line(ra, dec)
    # query_terms = set(sum([
    #     terms for (terms, segment) in line_to_segment_query_terms(
    #         catch.indexer, np.radians(ra), np.radians(dec), t)], []))

    # use three searches to benchmark each query step

    catch.source = 'skymapper'

    timestamps.append(("Fuzzy search\nby ephemeris", monotonic()))
    catch.find_observations_by_ephemeris(eph, approximate=True)

    timestamps.append(("Tests for\n3D intersection", monotonic()))
    dt = timestamps[-1][1] - timestamps[-2][1]  # do not double count fuzzy search
    print("dt", dt)
    catch.find_observations_by_ephemeris(eph, approximate=False)
    timestamps.append(("Done", monotonic() - dt))

t0 = timestamps[0][1]

fig, ax = plt.subplots(num=6, clear=True, figsize=(9, 5))
for i in range(len(timestamps)-1):
    t1 = timestamps[i][1] - t0
    t2 = timestamps[i+1][1] - t0
    bar = ax.barh(i, t2 - t1, 0.5, left=t1, zorder=100)
    ax.bar_label(bar, [f" +{t2 - t1:.1f}"])

ax.invert_yaxis()
plt.setp(ax, xlabel="Time (s)", xlim=[0, 11],
         yticks=range(len(timestamps) - 1),
         yticklabels=[t[0] for t in timestamps[:-1]],
         title=f"Skymapper search, {' to '.join(dates)}")

ax.tick_params("both", length=10)
ax.tick_params("x", which="minor", bottom=True)
ax.grid(axis="x", linewidth=0.75, zorder=-100)
plt.tight_layout()

fig.savefig(f"query-cells-waterfall-{file_suffix}.pdf")
fig.savefig(f"query-cells-waterfall-{file_suffix}.png", dpi=200)
fig.savefig(f"query-cells-waterfall-{file_suffix}.svg")

for i in range(len(timestamps) - 1):
    print(timestamps[i][0], timestamps[i][1],
          timestamps[i+1][1] - timestamps[i][1])
print(timestamps[-1][1] - timestamps[0][1])
