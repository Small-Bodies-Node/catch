# Licensed with the 3-clause BSD license.  See LICENSE for details.

import uuid
from typing import List

import pytest
import numpy as np
from astropy.time import Time
import sqlalchemy as sa
import testing.postgresql

from sbsearch.target import MovingTarget, FixedTarget
from ..catch import Catch
from ..config import Config
from ..model import (
    CatchQuery,
    NEATMauiGEODSS,
    NEATPalomarTricam,
    Found,
    SkyMapperDR4,
    CatalinaLemmon,
    Spacewatch,
    SurveyStats,
    PS1DR2,
)


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

    config = Config(database=postgresql.url(), log="/dev/null", debug=True)
    with Catch.with_config(config) as catch:
        catch.add_observations(observations)
        catch.update_statistics()


Postgresql = testing.postgresql.PostgresqlFactory(
    cache_initialized_db=True, on_initialized=dummy_surveys
)


@pytest.fixture(name="catch")
def fixture_catch():
    with Postgresql() as postgresql:
        config = Config(database=postgresql.url(), log="/dev/null", debug=True)
        with Catch.with_config(config) as catch:
            yield catch


def test_skymapper_dr4_url():
    obs = SkyMapperDR4(product_id="asdf")
    found = Found(ra=12.3, dec=-4.56)
    url = obs.cutout_url(found.ra, found.dec, size=0.1)
    assert url == (
        "https://api.skymapper.nci.org.au/public/siap/dr4/get_image?"
        f"IMAGE={obs.product_id}&SIZE=0.1&POS={found.ra},{found.dec}&FORMAT=fits"
    )


def test_css_url():
    obs = CatalinaLemmon(
        product_id="urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:g96_20220130_2b_n27011_01_0001.arch"
    )
    assert (
        obs.archive_url
        == "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.catalina.survey/data_calibrated/G96/2022/22Jan30/G96_20220130_2B_N27011_01_0001.arch.fz"
    )


def test_css_cutout_url():
    obs = CatalinaLemmon(
        product_id="urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:g96_20220130_2b_n27011_01_0001.arch"
    )
    found = Found(ra=12.3, dec=-4.56)

    url = obs.cutout_url(found.ra, found.dec, size=0.1)
    assert url == (
        "https://5ub5yo2kmj.execute-api.us-east-1.amazonaws.com/api/images/"
        "urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:g96_20220130_2b_n27011_01_0001.arch"
        "?ra=12.3&dec=-4.56&size=6.00arcmin&format=fits"
    )

    url = obs.preview_url(found.ra, found.dec, size=0.1)
    assert url == (
        "https://5ub5yo2kmj.execute-api.us-east-1.amazonaws.com/api/images/"
        "urn:nasa:pds:gbo.ast.catalina.survey:data_calibrated:g96_20220130_2b_n27011_01_0001.arch"
        "?ra=12.3&dec=-4.56&size=6.00arcmin&format=jpeg"
    )


def test_sw_url():
    obs = Spacewatch(
        product_id="urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1071_04.06_2009_07_29_03_59_40.003.fits",
        label="gbo.ast.spacewatch.survey/data/2009/07/29/sw_1071_04.06_2009_07_29_03_59_40.003.xml",
    )
    assert (
        obs.archive_url
        == "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/2009/07/29/sw_1071_04.06_2009_07_29_03_59_40.003.fits"
    )


def test_sw_cutout_url():
    obs = Spacewatch(
        product_id="urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1071_04.06_2009_07_29_03_59_40.003.fits"
    )
    found = Found(ra=12.3, dec=-4.56)

    url = obs.cutout_url(found.ra, found.dec, size=0.1)
    assert url == (
        "https://5ub5yo2kmj.execute-api.us-east-1.amazonaws.com/api/images/"
        "urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1071_04.06_2009_07_29_03_59_40.003.fits"
        "?ra=12.3&dec=-4.56&size=6.00arcmin&format=fits"
    )

    url = obs.preview_url(found.ra, found.dec, size=0.1)
    assert url == (
        "https://5ub5yo2kmj.execute-api.us-east-1.amazonaws.com/api/images/"
        "urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1071_04.06_2009_07_29_03_59_40.003.fits"
        "?ra=12.3&dec=-4.56&size=6.00arcmin&format=jpeg"
    )


def test_ps1dr2_url():
    obs = PS1DR2(
        product_id="rings.v3.skycell.1405.053.stk.g.unconv.fits",
        projection_id=1405,
        skycell_id=53,
    )

    url = obs.preview_url(ra=332.4875, dec=2.14639, size=0.025)
    assert url == (
        "https://ps1images.stsci.edu/cgi-bin/fitscut.cgi?"
        "red=%2Frings.v3.skycell%2F1405%2F053%2Frings.v3.skycell.1405.053.stk.g.unconv.fits"
        "&ra=332.4875&dec=2.14639&size=360&format=jpeg"
    )


def failed_search(self, *args):
    raise Exception


def test_source_time_limits(catch):
    mjd_start, mjd_stop = catch.db.session.query(
        sa.func.min(NEATMauiGEODSS.mjd_start),
        sa.func.max(NEATMauiGEODSS.mjd_stop),
    ).one()
    assert mjd_start == GEODSS_START
    assert np.isclose(mjd_stop, 50814.0 + 900 * (EXPTIME + SLEWTIME))

    mjd_start, mjd_stop = catch.db.session.query(
        sa.func.min(NEATPalomarTricam.mjd_start),
        sa.func.max(NEATPalomarTricam.mjd_stop),
    ).one()
    assert mjd_start == GEODSS_START + TRICAM_OFFSET
    assert np.isclose(mjd_stop, 50814.0 + 900 * (EXPTIME + SLEWTIME) + TRICAM_OFFSET)


@pytest.mark.remote_data
def test_query_moving_target(catch):
    # not yet searched --> not yet cached
    cached = catch.is_query_cached("2P")
    assert not cached

    job_id = uuid.uuid4()
    n = catch.query("2P", job_id)
    assert n == 2

    cached = catch.is_query_cached("2P")
    assert cached

    # should be 1 for each survey
    caught = catch.caught(job_id)
    assert len(caught) == 2

    # check query execution time
    queries = catch.queries_from_job_id(job_id)
    assert all([q.execution_time > 0 for q in queries])

    for source in (NEATMauiGEODSS, NEATPalomarTricam):
        assert sum([isinstance(c[1], source) for c in caught]) == 1

    # new query using a MovingTarget
    job_id = uuid.uuid4()
    target = MovingTarget("2P")
    m = catch.query(target, job_id)
    assert n == m

    # the query should now be cached
    cached = catch.is_query_cached("2P")
    assert cached

    # verify cache retrieval
    job_id = uuid.uuid4()
    m = catch.query(target, job_id)
    assert n == m

    # cached results do not store an execution time
    queries = catch.queries_from_job_id(job_id)
    assert all([q.execution_time is None for q in queries])


@pytest.mark.remote_data
def test_query_moving_target_date_range(catch, caplog):
    # cache a full survey query
    job_id = uuid.uuid4()
    target = MovingTarget("2P")
    n = catch.query(target, job_id)
    assert n == 2

    # repeat with a date range
    catch.start_date = Time(GEODSS_START - 100, format="mjd")
    job_id = uuid.uuid4()
    target = MovingTarget("2P")
    m = catch.query(target, job_id)
    assert n == m  # no change

    # more restrictive date range
    catch.stop_date = Time(GEODSS_START + TRICAM_OFFSET - 1, format="mjd")
    job_id = uuid.uuid4()
    target = MovingTarget("2P")
    n = catch.query(target, job_id)
    caught = catch.caught(job_id)
    assert n == 1
    assert isinstance(caught[0][1], NEATMauiGEODSS)

    # next query should be a cached result and just for Tricam
    catch.start_date = None
    catch.stop_date = None
    job_id = uuid.uuid4()
    n = catch.query("2P", job_id, sources=["neat_palomar_tricam"])
    assert n == 1

    assert (
        f"CATCH-APIs {job_id.hex}",
        20,
        "Added 1 cached result from NEAT Palomar Tricam.",
    ) in caplog.record_tuples

    caught = catch.caught(job_id)
    assert len(caught) == 1
    assert isinstance(caught[0][1], NEATPalomarTricam)

    # if the date range is outside the survey range, a cached result should be
    # returned

    # first, check start_date
    catch.start_date = Time(GEODSS_START + TRICAM_OFFSET - 300, format="mjd")
    catch.stop_date = None
    job_id = uuid.uuid4()

    cached = catch.is_query_cached("2P")
    assert cached

    n = catch.query("2P", job_id, sources=["neat_palomar_tricam"])
    assert n == 1

    # check stop_date
    catch.start_date = None
    catch.stop_date = Time(60000, format="mjd")
    job_id = uuid.uuid4()

    cached = catch.is_query_cached("2P")
    assert cached

    n = catch.query("2P", job_id, sources=["neat_palomar_tricam"])
    assert n == 1


@pytest.mark.remote_data
def test_query_moving_target_ephemeris_error(catch, caplog):
    # trigger ephemeris error
    job_id = uuid.uuid4()
    n = catch.query("2000P", job_id, sources=["neat_palomar_tricam"])
    assert n == 0
    assert (
        f"CATCH-APIs {job_id.hex}",
        40,
        "Could not get an ephemeris.",
    ) in caplog.record_tuples


@pytest.mark.remote_data
def test_query_moving_target_search_failure(catch, caplog, monkeypatch):
    # mock a search failure
    job_id = uuid.uuid4()
    with monkeypatch.context() as m:
        m.setattr(catch, "find_observations_by_ephemeris", failed_search)
        n = catch.query("2P", job_id, sources=["neat_palomar_tricam"], cached=False)
        assert n == 0
        assert (
            f"CATCH-APIs {job_id.hex}",
            40,
            "Critical error: could not search database for this target.",
        ) in caplog.record_tuples


@pytest.mark.remote_data
def test_cache(catch):
    cached = catch.is_query_cached("2P")
    assert not cached

    job_id = uuid.uuid4()
    n = catch.query("2P", job_id)
    assert n == 2

    cached = catch.is_query_cached("2P")
    assert cached

    # add padding, verify that the query has not been cached
    catch.padding = 0.001
    cached = catch.is_query_cached("2P")
    assert not cached

    # run the query with padding
    n = catch.query("2P", job_id)
    assert n == 2

    # was it cached?
    cached = catch.is_query_cached("2P")
    assert cached


def test_update_statistics(catch):
    catch.update_statistics()
    stats = (
        catch.db.session.query(SurveyStats)
        .filter(SurveyStats.source == "neat_maui_geodss")
        .one()
    )
    assert stats.count == 900

    all_stats: SurveyStats = (
        catch.db.session.query(SurveyStats).filter(SurveyStats.name == "All").one()
    )
    assert all_stats.count == 1800

    start = Time(GEODSS_START, format="mjd")
    stop = Time(GEODSS_START + EXPTIME * 900 + SLEWTIME * 899, format="mjd")
    assert stats.start_date == start.iso
    assert stats.stop_date == stop.iso

    fov = np.array(((-0.5, 0.5, 0.5, -0.5), (-0.5, -0.5, 0.5, 0.5))) * 5
    obs = NEATMauiGEODSS(
        mjd_start=stop.mjd,
        mjd_stop=stop.mjd + EXPTIME + SLEWTIME,
        product_id="asdf",
    )
    obs.set_fov(*fov)

    # add the new observation, update the other survey, and verify that the
    # GEODSS stats have not changed
    catch.add_observations([obs])
    catch.update_statistics(source="neat_palomar_tricam")
    stats = (
        catch.db.session.query(SurveyStats)
        .filter(SurveyStats.source == "neat_maui_geodss")
        .one()
    )
    assert stats.count == 900

    all_stats: SurveyStats = (
        catch.db.session.query(SurveyStats).filter(SurveyStats.name == "All").one()
    )
    assert all_stats.count == 1800

    # now update GEODSS and check stats
    catch.update_statistics(source="neat_maui_geodss")
    stats = (
        catch.db.session.query(SurveyStats)
        .filter(SurveyStats.source == "neat_maui_geodss")
        .one()
    )
    assert stats.count == 901
    start = Time(GEODSS_START, format="mjd")
    stop = Time(GEODSS_START + EXPTIME * 901 + SLEWTIME * 900, format="mjd")
    assert stats.start_date == start.iso
    assert stats.stop_date == stop.iso

    all_stats = (
        catch.db.session.query(SurveyStats).filter(SurveyStats.name == "All").one()
    )
    assert all_stats.count == 1801
    assert all_stats.start_date == start.iso
    stop = Time(
        GEODSS_START + TRICAM_OFFSET + EXPTIME * 900 + SLEWTIME * 899,
        format="mjd",
    )
    assert all_stats.stop_date == stop.iso
    assert all_stats.updated == stats.updated


def test_fixed_target_point_search(catch: Catch):
    target = FixedTarget.from_radec("00 05 00", "-30 15 00", unit=("hourangle", "deg"))
    job_id = uuid.uuid4()
    observations = catch.query(target, job_id)
    assert len(observations) == 4

    queries: List[CatchQuery] = catch.queries_from_job_id(job_id)
    assert all([query.intersection_type is None for query in queries])
    assert all([query.query == str(target) for query in queries])


def test_fixed_target_areal_search(catch: Catch):
    target = FixedTarget.from_radec("00 08 00", "-30 15 00", unit=("hourangle", "deg"))
    catch.padding = 180  # arcmin
    job_id = uuid.uuid4()
    observations = catch.query(target, job_id)
    assert len(observations) == 8

    queries: List[CatchQuery] = catch.queries_from_job_id(job_id)
    assert all([query.intersection_type == "ImageIntersectsArea" for query in queries])
    assert all([query.query == str(target) for query in queries])


def test_fixed_target_date_range(catch: Catch):
    target = FixedTarget.from_radec("00 05 00", "-30 15 00", unit=("hourangle", "deg"))
    job_id = uuid.uuid4()
    catch.stop_date = Time(
        52000, format="mjd"
    )  # just search dates before Palomar Tricam
    observations = catch.query(target, job_id)
    assert len(observations) == 2
    assert all([obs.source == "neat_maui_geodss" for obs in observations])
