# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import os
from warnings import warn
from collections import OrderedDict
import uuid

from sqlalchemy.orm.exc import NoResultFound
import numpy as np
from astropy.nddata import CCDData
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import Angle
from astropy.time import Time
import astropy.units as u

from sbsearch import SBSearch
from . import schema
from .schema import CatchQueries, Caught, Obs, Found, Obj


class CatchException(Exception):
    pass


class InvalidSessionID(CatchException):
    pass


class InvalidSourceName(CatchException):
    pass


class Catch(SBSearch):
    """CATCH survey search tool.


    Parameters
    ----------
    **kwargs
        `~sbsearch.SBSearch` keyword arguments.

    """

    SOURCES = {
        'neat palomar': schema.NEATPalomar,
        'neat geodss': schema.NEATMauiGEODSS,
    }

    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)

    def caught(self, job_id):
        """Return results from catch query.


        Parameters
        ----------
        job_id : uuid.UUID or string
            Unique job ID for original query.  UUID version 4.


        Returns
        -------
        rows : sqlalchemy Query
            Results as sqlalchemy objects: ``[Found, Obs, Obj]``.

        """

        job_id = uuid.UUID(str(job_id), version=4)

        rows = (self.db.session.query(Found, Obs, Obj)
                .join(Caught, Found.foundid == Caught.foundid)
                .join(CatchQueries, CatchQueries.queryid == Caught.queryid)
                .join(Obs, Found.obsid == Obs.obsid)
                .join(Obj, Found.objid == Obj.objid)
                .filter(CatchQueries.jobid == job_id.hex))

        return rows

    def drop(self, queryid):
        """Drop a caught object.


        Parameters
        ----------
        queryid : int
            Query ID.


        Returns
        -------
        n : int
            Number of rows deleted.

        """

        founds = (self.db.session.query(Found)
                  .join(Caught, Caught.foundid == Found.foundid)
                  .filter(Caught.queryid == queryid)
                  .all())
        n = len(founds)
        # cascades should delete catch_queries and caught rows
        for found in founds:
            self.db.session.delete(found)
        self.db.session.commit()
        return n

    def query(self, target, job_id, source='any', cached=True, **kwargs):
        """Try to catch an object in survey data.


        Parameters
        ----------
        target : string
            Target for which to search.

        job_id : uuid.UUID or string
            Unique ID for this query.  UUID version 4.

        source : string, optional
            Observation source table name.  See
            ``Catch.SOURCES.keys()`` for possible values, or use
            ``'any'`` to search each survey.

        cached : bool, optional
            Use cached results, if possible.

        **kwargs
            Any `~sbsearch.sbsearch.find_object` keyword except
            ``save``, ``update``, or ``location``.


        Returns
        -------
        count : int
            Number of observations found.

        """

        sources = self._validate_source(source)
        job_id = uuid.UUID(str(job_id), version=4)

        count = 0
        for source in sources:
            cached_query = self._find_catch_query(target, source)

            q = CatchQueries(query=str(target),
                             jobid=job_id.hex,
                             source=source,
                             date=Time.now().iso)
            self.db.session.add(q)
            self.db.session.commit()

            if cached and cached_query is not None:
                count += self._add_cached_results(q, cached_query)
                continue

            if cached_query is not None:
                n = self.drop(cached_query.queryid)
                self.logger.info('Dropped {} cached results for "{}"'
                                 .format(n, source))

            count += self._query(q, target, source, **kwargs)

        self.db.session.commit()
        return count

    def check_cache(self, target, source='any'):
        """Has this query been cached?


        Parameters
        ----------
        target : string
            Object to find.

        source : string, optional
            Survey source table name.  See ``Catch.SOURCES.keys()``
            for possible values, or use `'any'` to search each survey.


        Returns
        -------
        cached : bool
            ``True`` if the search is cached.  For ``'any'`` source,
            if any one survey search is missing, then the result is
            ``False``.

        """

        sources = self._validate_source(source)

        cached = True
        for source in sources:
            q = self._find_catch_query(target, source)
            cached *= q is not None

        return cached

    def _find_catch_query(self, target, source):
        """Find query ID for this target and source.

        Assumes the last search is the most relevant.

        """
        q = (self.db.session.query(CatchQueries)
             .filter(CatchQueries.query == target)
             .filter(CatchQueries.source == source)
             .order_by(CatchQueries.queryid.desc())
             .first())
        return q

    def _add_cached_results(self, query, cached_query):
        founds = (self.db.session.query(Found)
                  .join(Caught, Caught.foundid == Found.foundid)
                  .filter(Caught.queryid == cached_query.queryid)
                  .all())
        for found in founds:
            caught = Caught(
                queryid=query.queryid,
                foundid=found.foundid
            )
            self.db.session.add(caught)
        return len(founds)

    def _query(self, query, target, source, **kwargs):
        kwargs['save'] = True
        kwargs['update'] = True
        kwargs['source'] = self.SOURCES[source]
        kwargs['location'] = self.SOURCES[source].__obscode__

        obsids, foundids, newids = self.find_object(str(target), **kwargs)
        for foundid in foundids:
            caught = Caught(
                queryid=query.queryid,
                foundid=foundid
            )
            self.db.session.add(caught)

        self.logger.info(
            'Query {} caught {} observations of {} in "{}"'
            .format(query.queryid, len(obsids), target, source))

        self.db.session.commit()
        return len(foundids)

    def _validate_source(self, source):
        if source == 'any':
            sources = self.SOURCES.keys()
        else:
            sources = [source]

        for source in sources:
            if source not in self.SOURCES:
                raise InvalidSourceName(source)

        return sources

    def verify_database(self):
        super().verify_database([
            'catch_queries', 'caught',
            'neat_palomar',
            'neat_maui_geodss',
        ])
