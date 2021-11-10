import uuid
import numpy as np
import matplotlib.pyplot as plt
from astropy.time import Time
from catch import Catch, Config
from catch.model import SkyMapper

# Find 65P in SkyMapper DR2
#
# Catch v0 result:
#
# * JD: 2457971.9152
# * Product ID: 20170806095706-22
# * https://api.skymapper.nci.org.au/public/siap/dr2/get_image?IMAGE=20170806095706-22&SIZE=0.08333333333333333&POS=237.22441,-23.40757&FORMAT=fits
#
# For CATCH with min_edge_length = 3e-4 rad, spatial index terms are:
# $9e8c1,9e8c1,9e8c4,9e8d,9e8c,9e9,9ec,$9e8c7,9e8c7,$9e8ea04,9e8ea04,9e8ea1,9e8ea4,9e8eb,9e8ec,9e8f,$9e8ea0c,9e8ea0c,$9e8ea74,9e8ea74,9e8ea7

config = Config.from_file('../catch.config', debug=True)
with Catch.with_config(config) as catch:
    catch.db.engine.echo = False  # set to true to see SQL statements

    expected = (catch.db.session.query(SkyMapper)
                .filter(SkyMapper.product_id == '20170806095706-22')
                .all())[0]

    # benchmark queries
    t = []

    # full survey search
    t.append(Time.now())
    job_id = uuid.uuid4()
    count = catch.query('65P', job_id, sources=['skymapper'], cached=False,
                        debug=True)
    full = catch.caught(job_id)
    t.append(Time.now())

    comet = catch.get_designation('65P', add=True)
    eph = comet.ephemeris(SkyMapper, start=Time('2017-08-01'),
                          stop=Time('2017-09-01'))

    ra = np.radians([row.ra for row in eph])
    dec = np.radians([row.dec for row in eph])
    mjd = np.array([row.mjd for row in eph])
    query = catch.indexer.query_line(ra, dec)

    catch.logger.debug(
        '=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    t.append(Time.now())
    approx_no_time = catch.find_observations_intersecting_line(ra, dec)
    catch.logger.debug(
        '=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    t.append(Time.now())
    approx = catch.find_observations_by_ephemeris(eph, approximate=True)
    catch.logger.debug(
        '=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    t.append(Time.now())
    detailed = catch.find_observations_by_ephemeris(eph, approximate=False)
    catch.logger.debug(
        '=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    t.append(Time.now())

    dates = Time([obs.mjd_start for obs in
                  [expected] + detailed], format='mjd')
    eph0 = [comet.ephemeris(SkyMapper, dates=d)[0] for d in dates]

    print('Should ephemeris query find expected observation?',
          len(set(expected.spatial_terms).intersection(set(query))) > 0)

    print('Does ephemeris query match returned observations?',
          [len(set(row[1].spatial_terms).intersection(set(query))) > 0
           for row in full])

    print(f'''--------------------
catch.query: {(t[1] - t[0]).jd * 86400}

ephemeris: {(t[2] - t[1]).jd * 86400}

30-day period searches:
  line intersection: {(t[3] - t[2]).jd * 86400}
  find by ephemeris approximate: {(t[4] - t[3]).jd * 86400}
  find by ephemeris detailed: {(t[5] - t[4]).jd * 86400}
--------------------
''')

    catch.db.session.expunge_all()


def plot_obs(obs, **kwargs):
    ra, dec = np.array([c.split(':') for c in obs.fov.split(',')], float).T
    ra = np.r_[ra, ra[0]]
    dec = np.r_[dec, dec[0]]
    return plt.plot(ra, dec, label=obs.product_id, **kwargs)


plt.clf()
markers = 'os^'
for i, obs in enumerate([expected] + detailed):
    line = plot_obs(obs)[0]
    plt.scatter(eph0[i].ra, eph0[i].dec,
                color=line.get_color(), marker=markers[i],
                fc='none')

plt.plot(np.degrees(ra), np.degrees(dec), label='65P')

ax = plt.gca()
plt.setp(ax, xlim=ax.get_xlim()[::-1], xlabel='RA', ylabel='Dec')
plt.legend()
