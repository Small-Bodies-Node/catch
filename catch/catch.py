# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ["Catch"]

import time
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy.orm import Session, Query
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import func
from astropy.time import Time
from sbsearch import SBSearch, IntersectionType
from sbsearch.target import MovingTarget, FixedTarget

from .model import (
    CatchQuery,
    Observation,
    Found,
    Ephemeris,
    ExampleSurvey,
    SurveyStats,
)
from .exceptions import (
    CatchException,
    DataSourceWarning,
    FindObjectError,
    EphemerisError,
)
from .logging import TaskMessenger


class Catch(SBSearch):
    """CATCH survey search tool.


    Parameters
    ----------
    database : str or Session
        Database URL or initialized sqlalchemy Session.

    uncertainty_ellipse : bool, optional
        Search considering the uncertainty ellipse.

    padding : float, optional
        Additional padding to the search area, arcmin.

    debug : bool, optional
        Enable debugging messages.

    arc_limit : float, optional
        Maximal ephemeris arc length with which to search the database,
        radians.

    time_limit : float, optional
        Maximal ephemeris time length with which to search the database, days.

    """

    def __init__(
        self,
        database: Union[str, Session],
        *args,
        uncertainty_ellipse: bool = False,
        padding: float = 0,
        intersection: IntersectionType = IntersectionType.ImageIntersectsArea,
        **kwargs,
    ) -> None:
        super().__init__(
            database,
            *args,
            min_edge_length=1e-3,
            max_edge_length=0.017,
            uncertainty_ellipse=uncertainty_ellipse,
            padding=padding,
            intersection=intersection,
            logger_name="Catch",
            **kwargs,
        )

        # override sbsearch default logging behavior, which is a mix of DEBUG
        # and INFO.
        self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

        self._found_attributes = [
            attr
            for attr in dir(Found)
            if attr[0] != "_" and attr not in ["found_id", "query_id"]
        ]

    @property
    def sources(self) -> Dict[str, Observation]:
        """Dictionary of observation data sources in the information model.

        The dictionary is keyed by database table name.

        """
        return {
            source.__tablename__: source
            for source in Observation.__subclasses__()
            if source not in [ExampleSurvey]
        }

    def caught(self, job_id: Union[uuid.UUID, str]) -> List[Tuple[Found, Observation]]:
        """Return all results from catch query.


        Parameters
        ----------
        job_id : uuid.UUID or string
            Unique job ID for original query.  UUID version 4.


        Returns
        -------
        rows : list of tuples
            Results as sqlalchemy objects: ``(Found, Observation)``.

        """

        # get query identifiers for this job_id
        query_ids: List[int] = [q.query_id for q in self.queries_from_job_id(job_id)]

        # get results from Found
        rows: List[Tuple[Found, Observation]] = (
            self.db.session.query(Found, Observation)
            .join(Observation, Found.observation_id == Observation.observation_id)
            .filter(Found.query_id.in_(query_ids))
            .all()
        )

        return rows

    def queries_from_job_id(self, job_id: Union[uuid.UUID, str]) -> List[CatchQuery]:
        """Return list of `CatchQuery`s for the given `job_id`.


        Parameters
        ----------
        job_id : uuid.UUID or string
            Unique job ID for the query.  UUID version 4.


        Returns
        -------
        queries : list of CatchQuery objects

        """

        job_id: uuid.UUID = uuid.UUID(str(job_id), version=4)

        # find job_id in CatchQuery table
        queries: List[CatchQuery] = (
            self.db.session.query(CatchQuery)
            .filter(CatchQuery.job_id == job_id.hex)
            .all()
        )

        return queries

    def query(
        self,
        target: Union[str, MovingTarget, FixedTarget],
        job_id: Union[uuid.UUID, str],
        sources: Optional[str] = None,
        cached: bool = True,
    ) -> int:
        """Search for moving or fixed targets in survey data.

        Publishes messages to the Python logging system under the name
        'CATCH-APIs <job_id>'.


        Parameters
        ----------
        target : string, `MovingTarget`, `FixedTarget`
            Search for this target.  If a string, then it is assumed to be a
            moving target designation.

        job_id : `uuid.UUID` or string
            Unique ID for this query.  UUID version 4.

        sources : list of strings, optional
            Limit search to these sources.  See ``Catch.sources.keys()`` for
            possible values.

        cached : bool, optional
            Use cached results, if possible.  Has no effect for fixed target
            queries.


        Returns
        -------
        observations : int or list
            For moving targets, this is the number of observations found, for
            fixed targets this is the list of observations themselves.

        """

        # validate sources
        if sources is None:
            sources = self.sources.keys()
        else:
            for source in sources:
                try:
                    self.sources[source]
                except KeyError:
                    raise ValueError("Unknown source: {}")

        job_id = uuid.UUID(str(job_id), version=4)

        task_messenger: TaskMessenger = TaskMessenger(job_id, debug=self.debug)
        task_messenger.debug(
            "Searching for %s in %d survey%s.",
            target,
            len(sources),
            "" if len(sources) == 1 else "s",
        )

        observations: Union[int, List[Observation]]
        if isinstance(target, FixedTarget):
            observations = self._query_fixed_target(
                target, job_id, sources, task_messenger
            )
        else:
            observations = self._query_moving_target(
                target, job_id, sources, cached, task_messenger
            )

        return observations

    def _query_moving_target(
        self,
        target: Union[str, MovingTarget],
        job_id: Union[uuid.UUID, str],
        sources: List[str],
        cached: bool,
        task_messenger: TaskMessenger,
    ) -> int:
        """Search for moving targets.


        Returns
        -------
        count : int
            Number of observations found.

        """

        count: int = 0
        for source in sources:
            # track query execution time
            execution_time: float = time.monotonic()

            self.source = source
            source_name = self.source.__data_source_name__
            self.logger.debug("Query {}".format(source_name))

            cached_query = self._find_catch_query(target)

            q = CatchQuery(
                query=str(target),
                job_id=job_id.hex,
                source=self.source.__tablename__,
                date=Time.now().iso,
                status="in progress",
                uncertainty_ellipse=self.uncertainty_ellipse,
                padding=self.padding,
                intersection=None,  # not used for moving targets
            )
            self.db.session.add(q)
            self.db.session.commit()

            if cached and cached_query is not None:
                n = self._copy_cached_results(q, cached_query)
                count += n
                task_messenger.send(
                    "Added %d cached result%s from %s.",
                    n,
                    "" if n == 1 else "s",
                    source_name,
                )
                q.status = "finished"
                self.db.session.commit()
            else:
                try:
                    n = self._find_and_cache_moving_target_observations(
                        q, target, task_messenger
                    )
                except DataSourceWarning as e:
                    task_messenger.send(str(e))
                    q.status = "finished"
                except CatchException as e:
                    q.status = "errored"
                    task_messenger.error(str(e))
                    self.logger.error(e, exc_info=self.debug)
                else:
                    count += n
                    task_messenger.send(
                        "Caught %d observation%s.", n, "" if n == 1 else "s"
                    )
                    q.status = "finished"
                finally:
                    q.execution_time = time.monotonic() - execution_time
                    self.db.session.commit()

        return count

    def _query_fixed_target(
        self,
        target: FixedTarget,
        job_id: Union[uuid.UUID, str],
        sources: List[str],
        task_messenger: TaskMessenger,
    ) -> List[Observation]:
        """Search for fixed targets.


        Returns
        -------
        observations : list
            Observations of the target.

        """

        # track query execution time
        execution_time: float = time.monotonic()

        self.source = Observation
        self.logger.debug("Query {}".format(", ".join(sources)))

        q = CatchQuery(
            query=str(target),
            job_id=job_id.hex,
            source=",".join(sources),
            date=Time.now().iso,
            status="in progress",
            uncertainty_ellipse=0,
            padding=self.padding,
            intersection=self.intersection.value,
        )
        self.db.session.add(q)
        self.db.session.commit()

        observations: List[Observation] = []
        try:
            observations = self._get_fixed_target_observations(
                target, radius, intersection, task_messenger, sources
            )
        except DataSourceWarning as e:
            task_messenger.send(str(e))
            q.status = "finished"
        except CatchException as e:
            q.status = "errored"
            task_messenger.error(str(e))
            self.logger.error(e, exc_info=self.debug)
        else:
            n = len(observations)
            task_messenger.send("Caught %d observation%s.", n, "" if n == 1 else "s")
            q.status = "finished"
        finally:
            q.execution_time = time.monotonic() - execution_time
            self.db.session.commit()

        return observations

    def is_query_cached(self, target: str, sources: Optional[str] = None) -> str:
        """Determine if this query has already been cached.


        ``uncertainty_ellipse`` and ``padding`` parameters are also checked.


        Parameters
        ----------
        target : string
            Target for which to search.

        sources : list of strings, optional
            Limit search to these sources.  See ``Catch.sources.keys()``
            for possible values.


        Returns
        -------
        cached : bool
            ``False`` if any source specified by ``sources`` is not yet
            cached for this ``target``.

        """

        # validate sources
        if sources is None:
            sources = self.sources.keys()
        else:
            for source in sources:
                try:
                    self.sources[source]
                except KeyError:
                    raise ValueError("Unknown source: {}")

        cached: bool = True  # assume cached until proven otherwise
        for source in sources:
            self.source = source
            cached = self._find_catch_query(target) is not None

        return cached

    def update_statistics(self, source=None):
        """Update source survey statistics table.


        Parameters
        ----------
        source : string or Observation object
            Limit update to this survey source name.

        """

        sources: List[Observation]
        if source is None:
            # update everything
            sources = list(self.sources.values())
        else:
            # just the requested table
            sources = [self.sources.get(source, source)]

        for _source in sources:
            self._update_statistics(_source)

        self._update_statistics_all()

        self.db.session.commit()

    def _update_statistics(self, source):
        count: int = self.db.session.query(func.count(source.observation_id)).scalar()

        q: Query = self.db.session.query(
            func.min(Observation.mjd_start), func.max(Observation.mjd_stop)
        ).filter(Observation.source == source.__tablename__)
        dates: Any = q.one()

        table_name = source.__tablename__
        source_name = source.__data_source_name__

        stats: SurveyStats
        try:
            stats = (
                self.db.session.query(SurveyStats)
                .filter(SurveyStats.source == table_name)
                .one()
            )
        except NoResultFound:
            stats = SurveyStats(
                source=table_name,
                name=source_name,
            )

        stats.count = count
        if count > 0:
            stats.start_date = Time(dates[0], format="mjd").iso
            stats.stop_date = Time(dates[1], format="mjd").iso
        stats.updated = Time.now().iso

        self.db.session.merge(stats)

    def _update_statistics_all(self):
        # update the 'All' entry in survey_statistics
        q: Query = self.db.session.query(
            func.sum(SurveyStats.count),
            func.min(SurveyStats.start_date),
            func.max(SurveyStats.stop_date),
            func.max(SurveyStats.updated),
        ).filter(SurveyStats.name != "All")
        updated_stats: Any = q.one()

        stats: SurveyStats
        try:
            stats = (
                self.db.session.query(SurveyStats)
                .filter(SurveyStats.name == "All")
                .one()
            )
        except NoResultFound:
            stats = SurveyStats(
                source="",
                name="All",
            )

        stats.count = updated_stats[0]
        stats.start_date = updated_stats[1]
        stats.stop_date = updated_stats[2]
        stats.updated = updated_stats[3]
        self.db.session.merge(stats)

    def _find_catch_query(
        self, target: Union[str, MovingTarget]
    ) -> Union[CatchQuery, None]:
        """Find query ID for this moving target and source.

        ``uncertainty_ellipse``, and ``padding`` parameters are also checked.

        Returns the last search with status=='finished', ``None`` otherwise.

        """

        q: int = (
            self.db.session.query(CatchQuery)
            .filter(CatchQuery.query == str(target))
            .filter(CatchQuery.source == self.source.__tablename__)
            .filter(CatchQuery.status == "finished")
            .filter(CatchQuery.uncertainty_ellipse == self.uncertainty_ellipse)
            .filter(
                CatchQuery.padding.between(self.padding * 0.99, self.padding * 1.01)
            )
            .order_by(CatchQuery.query_id.desc())
            .first()
        )

        return q

    def _copy_cached_results(self, query: CatchQuery, cached_query: CatchQuery) -> int:
        """Copy previously cached results to a new query.

        Returns
        -------
        n : int
            Number of copied rows.

        """

        founds: List[Found] = (
            self.db.session.query(Found)
            .filter(Found.query_id == cached_query.query_id)
            .all()
        )

        found: Found
        for i in range(len(founds)):
            found = Found(query_id=query.query_id)
            for k in self._found_attributes:
                setattr(found, k, getattr(founds[i], k))
            self.db.session.add(found)

        return len(founds)

    def _find_and_cache_moving_target_observations(
        self,
        query: CatchQuery,
        target: Union[str, MovingTarget],
        task_messenger: TaskMessenger,
    ):
        """Run the actual query.

        1. Find the date range of the current survey.  If ``None`` then the
           survey has no observations: silently return no results.

        2. Notify the user of the survey and date range being searched.

        3. Get the target's ephemeris over the survey date range.

        4. If the ephemeris query succeeded, then this is a good target.  Save
           it to the target database.

        5. Query the database for observations of the target ephemeris.

        6. Observations found?  Then add them to the found table.

        """

        # date range for this survey
        q: Query = self.db.session.query(
            func.min(Observation.mjd_start), func.max(Observation.mjd_stop)
        )
        if self.source.__tablename__ != "observation":
            q = q.filter(Observation.source == self.source.__tablename__)

        mjd_start: float
        mjd_stop: float
        mjd_start, mjd_stop = q.one()

        if None in [mjd_start, mjd_stop]:
            raise DataSourceWarning(
                f"No observations to search in database for {self.source.__data_source_name__}."
            )

        # notify the user of survey and date range being searched
        task_messenger.send(
            "Query %s from %s to %s.",
            self.source.__data_source_name__,
            Time(mjd_start, format="mjd").iso[:10],
            Time(mjd_stop, format="mjd").iso[:10],
        )

        # get target ephemeris
        _target: MovingTarget = MovingTarget(str(target), db=self.db)
        try:
            eph: List[Ephemeris] = _target.ephemeris(
                self.source.__obscode__,
                start=Time(mjd_start - 1, format="mjd"),
                stop=Time(mjd_stop + 1, format="mjd"),
            )
        except Exception as e:
            raise EphemerisError("Could not get an ephemeris.") from e
        self.logger.info("Obtained ephemeris from JPL Horizons.")
        task_messenger.send("Obtained ephemeris from JPL Horizons.")

        # ephemeris was successful, add target to database, if needed
        _target = self.get_designation(str(_target), add=True)

        # Query the database for observations of the target ephemeris
        try:
            observations: List[self.source] = self.find_observations_by_ephemeris(eph)
        except Exception as e:
            raise FindObjectError(
                "Critical error: could not search database for this target."
            ) from e

        if len(observations) > 0:
            # Observations found?  Then add them to the found table.
            founds: List[Found] = self.add_found(_target, observations)

            # include query_id
            found: Found
            for found in founds:
                found.query_id = query.query_id

            self.db.session.commit()

            return len(founds)
        else:
            return 0

    def _get_fixed_target_observations(
        self,
        target: FixedTarget,
        task_messenger: TaskMessenger,
        sources: List[str],
    ) -> List[Observation]:
        """Run the actual query.

        1. Notify the user of the survey and date range being searched.

        2. Query the database for observations.

        """

        # notify the user of survey and date range being searched
        task_messenger.send("Query %s.", ", ".join(sources))

        # Query the database for observations of the target ephemeris
        observations: List[Observation]
        try:
            if self.padding > 0:
                observations = self.find_observations_containing_point(target)
            else:
                observations = self.find_observations_intersecting_cap(target)
        except Exception as e:
            raise FindObjectError(
                "Critical error: could not search database for this target."
            ) from e

        # limit results to requested sources
        observations = [obs for obs in observations if obs.source in sources]

        return observations
