# Licensed with the 3-clause BSD license.  See LICENSE for details.

import uuid
import pytest

import numpy as np

from ..catch import Catch
from ..config import Config
from ..model import (NEATMauiGEODSS, NEATPalomarTricam, Ephemeris, Found,
                     SkyMapper)


def dummy_surveys():
    mjd_start = 50814.0
    exp = 30 / 86400
    fov = np.array(((-0.5, 0.5, 0.5, -0.5), (-0.5, -0.5, 0.5, 0.5))) * 5
    observations = []
    for dec in np.linspace(-30, 90, 36):
        for ra in np.linspace(0, 360, int(36 * np.cos(np.radians(dec)))):
            _fov = fov + np.array([[ra], [dec]])
            obs = NEATMauiGEODSS(
                mjd_start=mjd_start,
                mjd_stop=mjd_start + exp,
            )
            obs.set_fov(*_fov)
            observations.append(obs)

            obs = NEATPalomarTricam(
                mjd_start=mjd_start + 1461,
                mjd_stop=mjd_start + exp + 1461,
            )
            obs.set_fov(*_fov)
            observations.append(obs)

            mjd_start += exp + 7 / 86400

    return observations


def test_skymapper_url():
    obs = SkyMapper(
        product_id='asdf'
    )
    found = Found(
        ra=12.3,
        dec=-4.56
    )
    url = Catch.skymapper_cutout_url(found, obs, size=0.1)
    assert url == ('https://api.skymapper.nci.org.au/public/siap/dr2/get_image?'
                   f'IMAGE={obs.product_id}&SIZE=0.1&POS={found.ra},{found.dec}&FORMAT=fits')


@pytest.fixture(name='catch')
def fixture_catch():
    config = Config(database='sqlite://',
                    log='/dev/null', debug=True)
    with Catch.with_config(config) as catch:
        catch.add_observations(dummy_surveys())
        yield catch


def failed_search(self, *args):
    raise Exception


def test_query_all(catch, caplog, monkeypatch):
    job_id = uuid.uuid4()
    n = catch.query('2P', job_id)
    assert n == 2

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
