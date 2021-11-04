# Licensed with the 3-clause BSD license.  See LICENSE for details.

import uuid
from numpy.core.fromnumeric import product
import pytest

import numpy as np
from astropy.tests.helper import remote_data
import sqlalchemy as sa
import testing.postgresql

from ..catch import Catch
from ..config import Config
from ..model import (NEATMauiGEODSS, NEATPalomarTricam, Found, SkyMapper)


# dummy_surveys survey parameters
GEODSS_START = 50814.0
TRICAM_OFFSET = 1461
EXPTIME = 30 / 86400
SLEWTIME = 7 / 86400


def dummy_surveys(postgresql):
    mjd_start = GEODSS_START
    fov = np.array(((-0.5, 0.5, 0.5, -0.5), (-0.5, -0.5, 0.5, 0.5))) * 5
    observations = []
    product_id = 0
    for dec in np.linspace(-30, 90, 36):
        for ra in np.linspace(0, 360, int(36 * np.cos(np.radians(dec)))):
            product_id += 1
            _fov = fov + np.array([[ra], [dec]])
            obs = NEATMauiGEODSS(
                mjd_start=mjd_start,
                mjd_stop=mjd_start + EXPTIME,
                product_id=product_id,
            )
            obs.set_fov(*_fov)
            observations.append(obs)

            obs = NEATPalomarTricam(
                mjd_start=mjd_start + TRICAM_OFFSET,
                mjd_stop=mjd_start + EXPTIME + TRICAM_OFFSET,
                product_id=product_id,
            )
            obs.set_fov(*_fov)
            observations.append(obs)

            mjd_start += EXPTIME + SLEWTIME

    config = Config(database=postgresql.url(), log='/dev/null', debug=True)
    with Catch.with_config(config) as catch:
        catch.add_observations(observations)


Postgresql = testing.postgresql.PostgresqlFactory(
    cache_initialized_db=True, on_initialized=dummy_surveys)


@pytest.fixture(name='catch')
def fixture_catch():
    with Postgresql() as postgresql:
        config = Config(database=postgresql.url(), log='/dev/null', debug=True)
        with Catch.with_config(config) as catch:
            yield catch


def test_skymapper_url():
    obs = SkyMapper(
        product_id='asdf'
    )
    found = Found(
        ra=12.3,
        dec=-4.56
    )
    url = obs.cutout_url(found.ra, found.dec, size=0.1)
    assert url == ('https://api.skymapper.nci.org.au/public/siap/dr2/get_image?'
                   f'IMAGE={obs.product_id}&SIZE=0.1&POS={found.ra},{found.dec}&FORMAT=fits')


def failed_search(self, *args):
    raise Exception


def test_source_time_limits(catch):
    mjd_start, mjd_stop = catch.db.session.query(
        sa.func.min(NEATMauiGEODSS.mjd_start),
        sa.func.max(NEATMauiGEODSS.mjd_stop)
    ).one()
    assert mjd_start == GEODSS_START
    assert np.isclose(mjd_stop, 50814.0 + 900 * (EXPTIME + SLEWTIME))

    mjd_start, mjd_stop = catch.db.session.query(
        sa.func.min(NEATPalomarTricam.mjd_start),
        sa.func.max(NEATPalomarTricam.mjd_stop)
    ).one()
    assert mjd_start == GEODSS_START + TRICAM_OFFSET
    assert np.isclose(mjd_stop, 50814.0 + 900 * (EXPTIME + SLEWTIME)
                      + TRICAM_OFFSET)


@remote_data
def test_query_all(catch, caplog, monkeypatch):
    # test data only exists for:
    source_keys = None  # ['neat_palomar_tricam', 'neat_maui_geodss']

    cached = catch.is_query_cached('2P')
    assert not cached

    job_id = uuid.uuid4()
    n = catch.query('2P', job_id, source_keys=source_keys)
    assert n == 2

    cached = catch.is_query_cached('2P')
    assert cached

    # should be 1 for each survey
    caught = catch.caught(job_id)
    assert len(caught) == 2

    for source in (NEATMauiGEODSS, NEATPalomarTricam):
        assert sum([isinstance(c[1], source) for c in caught]) == 1

    # second query should be a cached result
    job_id = uuid.uuid4()
    n = catch.query('2P', job_id, source_keys=['neat_palomar_tricam'])
    assert n == 1

    assert (
        (f'CATCH-APIs {job_id.hex}', 20,
         'Added 1 cached result from NEAT Palomar Tricam.')
        in caplog.record_tuples
    )

    caught = catch.caught(job_id)
    assert len(caught) == 1
    assert isinstance(caught[0][1], NEATPalomarTricam)

    # trigger ephemeris error
    job_id = uuid.uuid4()
    n = catch.query('2000P', job_id, source_keys=['neat_palomar_tricam'])
    assert n == 0
    assert (
        (f'CATCH-APIs {job_id.hex}', 40, 'Could not get an ephemeris.')
        in caplog.record_tuples
    )

    # mock a search failure
    job_id = uuid.uuid4()
    with monkeypatch.context() as m:
        m.setattr(catch, "find_observations_by_ephemeris", failed_search)
        n = catch.query('2P', job_id, source_keys=['neat_palomar_tricam'],
                        cached=False)
        assert n == 0
        assert (
            (f'CATCH-APIs {job_id.hex}', 40,
             'Critical error: could not search database for this target.')
            in caplog.record_tuples
        )
