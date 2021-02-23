# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import uuid
import logging
from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy.orm import Session
from sqlalchemy import func
from astropy.time import Time
from sbsearch import SBSearch
from sbsearch.target import MovingTarget

from .model import (CatchQuery, Caught, Observation, Found, Ephemeris,
                    UnspecifiedSurvey)
from .exceptions import CatchException, FindObjectError, EphemerisError
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

    """

    def __init__(self, database: Union[str, Session], *args,
                 uncertainty_ellipse: bool = False, padding: float = 0,
                 debug: bool = False, **kwargs) -> None:
        # fixed min_edge_length value (1 arcmin)
        super().__init__(database, *args, min_edge_length=3e-4,
                         uncertainty_ellipse=uncertainty_ellipse,
                         padding=padding, logger_name='Catch', **kwargs)
        self.debug: bool = debug
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    @property
    def sources(self) -> Dict[str, Observation]:
        """Dictionary of observation data sources in the information model.

        The dictionary is keyed by database table name.

        """
        # skip UnspecifiedSurvey
        return {
            source.__tablename__: source
            for source in [Observation] + Observation.__subclasses__()
            if source not in [UnspecifiedSurvey]
        }

    def caught(self, job_id: Union[uuid.UUID, str]
               ) -> List[Tuple[Found, Observation]]:
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

        job_id: uuid.UUID = uuid.UUID(str(job_id), version=4)

        rows: List[Tuple[Found, Observation]] = (
            self.db.session.query(Found, Observation)
            .join(Caught, Found.found_id == Caught.found_id)
            .join(CatchQuery, CatchQuery.query_id == Caught.query_id)
            .join(Observation, Found.observation_id == Observation.observation_id)
            .filter(CatchQuery.job_id == job_id.hex)
            .all()
        )

        return rows

    def query(self, target: str, job_id: Union[uuid.UUID, str],
              source_keys: Optional[str] = None, cached: bool = True,
              **kwargs) -> int:
        """Try to catch an object in survey data.

        Publishes messages to the Python logging system under the name
        'CATCH-APIs <job_id>'.


        Parameters
        ----------
        target : string
            Target for which to search.

        job_id : uuid.UUID or string
            Unique ID for this query.  UUID version 4.

        source_keys : list of strings, optional
            Limit search to these sources.  See ``Catch.sources.keys()``
            for possible values.

        cached : bool, optional
            Use cached results, if possible.


        Returns
        -------
        count : int
            Number of observations found.

        """

        source_keys = (
            list(self.sources.keys())
            if source_keys is None
            else source_keys
        )
        sources: List[Observation] = [
            self.sources[name] for name in source_keys
            if name != 'observation'
        ]
        job_id = uuid.UUID(str(job_id), version=4)

        task_messenger: TaskMessenger = TaskMessenger(job_id, debug=self.debug)
        task_messenger.debug('Searching for %s in %d survey%s.',
                             target, len(sources),
                             "" if len(sources) == 1 else "s")

        count = 0
        for source in sources:
            self.source = source
            source_name = source.__data_source_name__
            self.logger.debug('Query {}'.format(source_name))

            cached_query = self._find_catch_query(target, source)

            q = CatchQuery(
                query=str(target),
                job_id=job_id.hex,
                source=source.__tablename__,
                date=Time.now().iso,
                status='in progress',
                uncertainty_ellipse=self.uncertainty_ellipse,
                padding=self.padding
            )
            self.db.session.add(q)
            self.db.session.commit()

            if cached and cached_query is not None:
                n = self._copy_cached_results(q, cached_query)
                count += n
                task_messenger.send('Added %d cached result%s from %s.',
                                    n, '' if n == 1 else 's', source_name)
                q.status = 'finished'
                self.db.session.commit()
            else:
                try:
                    n = self._query(q, target)
                except CatchException as e:
                    q.status = 'errored'
                    task_messenger.error(str(e))
                    self.logger.error(e)
                else:
                    count += n
                    task_messenger.send('Caught %d observation%s in %s.',
                                        n, '' if n == 1 else 's', source_name)
                    q.status = 'finished'
                finally:
                    self.db.session.commit()

        return count

    def is_query_cached(self, target: str, source_keys: Optional[str] = None
                        ) -> str:
        """Determine if this query has already been cached.


        ``uncertainty_ellipse`` and ``padding`` parameters are also checked.


        Parameters
        ----------
        target : string
            Target for which to search.

        source_keys : list of strings, optional
            Limit search to these sources.  See ``Catch.sources.keys()``
            for possible values.


        Returns
        -------
        cached : bool
            ``False`` if any source specified by ``source_keys`` is not yet
            cached for this ``target``.

        """

        source_keys = (
            list(self.sources.keys())
            if source_keys is None
            else source_keys
        )

        sources: List[Observation] = [
            self.sources[name] for name in source_keys
            if name != 'observation'
        ]

        cached: bool = True  # assume cached until proven otherwise
        for source in sources:
            self.source = source
            cached = self._find_catch_query(target, source) is not None

        return cached

    def _find_catch_query(self, target: str, source: Observation
                          ) -> Union[CatchQuery, None]:
        """Find query ID for this target and source.

        ``uncertainty_ellipse`` and ``padding`` parameters are also checked.

        Returns the last search with status=='finished', ``None`` otherwise.

        """

        q: int = (
            self.db.session.query(CatchQuery)
            .filter(CatchQuery.query == target)
            .filter(CatchQuery.source == source.__tablename__)
            .filter(CatchQuery.status == 'finished')
            .filter(CatchQuery.uncertainty_ellipse == self.uncertainty_ellipse)
            .filter(CatchQuery.padding == self.padding)
            .order_by(CatchQuery.query_id.desc())
            .first()
        )

        return q

    def _copy_cached_results(self, query: CatchQuery,
                             cached_query: CatchQuery) -> int:
        """Copy previously cached results to a new query.

        Returns
        -------
        n : int
            Number of copied rows.

        """

        founds: List[Found] = (
            self.db.session.query(Found)
            .join(Caught, Caught.found_id == Found.found_id)
            .filter(Caught.query_id == cached_query.query_id)
            .all()
        )

        found: Found
        for found in founds:
            self.db.session.add(
                Caught(
                    query_id=query.query_id,
                    found_id=found.found_id
                )
            )

        return len(founds)

    def _query(self, query: CatchQuery, target: str):
        # date range for this survey
        mjd_start: float
        mjd_stop: float
        mjd_start, mjd_stop = self.db.session.query(
            func.min(self.source.mjd_start),
            func.max(self.source.mjd_stop)
        ).one()

        if None in [mjd_start, mjd_stop]:
            # no observations in the database for this source
            return 0

        target: MovingTarget = MovingTarget(target)
        try:
            eph: List[Ephemeris] = target.ephemeris(
                self.source.__obscode__,
                start=Time(mjd_start - 1, format='mjd'),
                stop=Time(mjd_stop + 1, format='mjd')
            )
        except Exception as e:
            raise EphemerisError('Could not get an ephemeris.') from e

        try:
            observations: List[self.source] = (
                self.find_observations_by_ephemeris(eph)
            )
        except Exception as e:
            raise FindObjectError(
                'Critical error: could not search database for this target.') from e

        n: int = 0
        if len(observations) > 0:
            found: Found
            for found in self.add_found(target, observations):
                caught = Caught(
                    query_id=query.query_id,
                    found_id=found.found_id
                )
                self.db.session.add(caught)
                n += 1

        self.db.session.commit()
        return n
