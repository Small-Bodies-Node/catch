# Licensed with the 3-clause BSD license.  See LICENSE for details.

"""Source, query, and database statistics."""

from sqlalchemy.orm import Query
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import func, Row

from astropy.time import Time

from .model import SurveyStats, Observation, CatchQuery
from .catch import Catch


def source_statistics(catch: Catch) -> list[dict[str, str | int | None]]:
    """Get source statistics from survey statistics table."""

    rows = []
    stat: SurveyStats
    for stat in catch.db.session.query(SurveyStats).order_by(SurveyStats.name).all():
        rows.append(
            {
                "source": stat.source,
                "name": stat.name,
                "start-date": stat.start_date,
                "stop-date": stat.stop_date,
                "count": stat.count,
            }
        )

    return rows


def recently_added_observations(
    catch: Catch,
    sources: list[str] | None = None,
) -> list[dict[str, str | int | None]]:
    """Summarize recent additions to the CATCH observations database.


    Parameters
    ----------
    catch : Catch
        The CATCH instance to summarize.

    sources : list of str, optional
        Only summarize these sources.  Default is to summarize everything known
        to `catch`.


    Returns
    -------
    summary : list of dict

    """

    if sources is None:
        sources = catch.sources.keys()

    summary: list[dict[str, str | int | None]] = []
    t0: float = Time.now().mjd
    for n in [1, 7, 30]:
        results: list[Row] = (
            catch.db.session.query(
                Observation.source,
                func.count(Observation.mjd_start).label("count"),
                func.min(Observation.mjd_start).label("start_date"),
                func.max(Observation.mjd_stop).label("stop_date"),
            )
            .where(Observation.mjd_added > (t0 - n))
            .group_by(Observation.source)
            .all()
        )
        for row in results:
            if row.source in sources:
                summary.append(
                    {
                        "source": row.source,
                        "source_name": catch.sources[row.source].__data_source_name__,
                        "days": n,
                        "count": row.count,
                        "start_date": Time(row.start_date, format="mjd").iso,
                        "stop_date": Time(row.stop_date, format="mjd").iso,
                    }
                )
    return summary


def recent_queries(
    catch: Catch,
    sources: list[str] | None = None,
) -> list[dict[str, str | int]]:
    """Summarize recent CATCH queries.


    Parameters
    ----------
    catch : Catch
        The CATCH instance to summarize.

    sources : list of str, optional
        Only count these sources.  Default is to count anything known to
        `catch`.


    Returns
    -------
    summary : list of dict

    """

    if sources is None:
        sources = catch.sources.keys()

    t0 = Time.now().mjd

    summary: list[dict[str, str | int | None]] = []
    for n in [1, 7, 30]:
        # only summarize sources known to us
        q = catch.db.session.query(
            func.count(func.distinct(CatchQuery.job_id)).label("jobs"),
            func.count((CatchQuery.status == "finished") | None).label("finished"),
            (
                func.count(CatchQuery.status == "finished")
                - func.count(CatchQuery.execution_time)
            ).label("cached"),
            func.count((CatchQuery.status == "errored") | None).label("errored"),
            func.count((CatchQuery.status == "in progress") | None).label(
                "in_progress"
            ),
        ).where(
            (CatchQuery.date > Time(t0 - n, format="mjd").iso)
            & CatchQuery.source.in_(sources)
        )

        result: Row = q.one()

        summary.append(
            {
                "days": n,
                "jobs": result.jobs,
                "finished": result.finished,
                "cached": result.cached,
                "errored": result.errored,
                "in_progress": result.in_progress,
            }
        )
    return summary


def update_statistics(catch: Catch, source: str | None = None) -> None:
    """Update source survey statistics table.


    Parameters
    ----------
    source : string or Observation object
        Limit update to this survey source name.

    """

    sources: list[Observation]
    if source is None:
        # update everything
        sources = list(catch.sources.values())
    else:
        # just the requested table
        sources = [catch.sources.get(source, source)]

    for s in sources:
        update_statistics_for_source(catch, s)

    update_statistics_for_all(catch)

    catch.db.session.commit()


def update_statistics_for_source(catch: Catch, source: Observation) -> None:
    """Update statistics for a single source."""

    # offset to MJD for calculating local night
    night_offset: float = (
        source.__night_offset__ if hasattr(source, "__night_offset__") else 0
    ) - 0.5

    count: int = catch.db.session.query(source.observation_id).count()

    q: Query = catch.db.session.query(
        func.min(Observation.mjd_start), func.max(Observation.mjd_stop)
    ).filter(Observation.source == source.__tablename__)
    dates: Row = q.one()

    nights: int = (
        catch.db.session.query(func.floor(Observation.mjd_start + night_offset))
        .filter(Observation.source == source.__tablename__)
        .distinct()
        .count()
    )

    table_name = source.__tablename__
    source_name = source.__data_source_name__

    stats: SurveyStats
    try:
        stats = (
            catch.db.session.query(SurveyStats)
            .filter(SurveyStats.source == table_name)
            .one()
        )
    except NoResultFound:
        stats = SurveyStats(
            source=table_name,
            name=source_name,
        )

    stats.count = count
    stats.nights = nights
    if count > 0:
        stats.start_date = Time(dates[0], format="mjd").iso
        stats.stop_date = Time(dates[1], format="mjd").iso
    stats.updated = Time.now().iso

    catch.db.session.merge(stats)


def update_statistics_for_all(catch: Catch) -> None:
    """Update aggregated statistics for all sources."""

    q: Query = catch.db.session.query(
        func.sum(SurveyStats.count),
        func.min(SurveyStats.start_date),
        func.max(SurveyStats.stop_date),
        func.max(SurveyStats.updated),
    ).filter(SurveyStats.name != "All")
    updated_stats: Row = q.one()

    nights: int = (
        catch.db.session.query(func.floor(Observation.mjd_start - 0.5))
        .distinct()
        .count()
    )

    stats: SurveyStats
    try:
        stats = (
            catch.db.session.query(SurveyStats).filter(SurveyStats.name == "All").one()
        )
    except NoResultFound:
        stats = SurveyStats(
            source="",
            name="All",
        )

    stats.count = updated_stats[0]
    stats.nights = nights
    stats.start_date = updated_stats[1]
    stats.stop_date = updated_stats[2]
    stats.updated = updated_stats[3]

    catch.db.session.merge(stats)
