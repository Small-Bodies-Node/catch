# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import os
from warnings import warn
from collections import OrderedDict

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


class CatchException:
    pass


class InvalidSessionID(CatchException):
    pass


class Catch(SBSearch):
    """CATCH survey search tool.


    Parameters
    ----------
    **kwargs
        `~sbsearch.SBSearch` keyword arguments.

    """

    SURVEYS = {
        'neat palomar': schema.NEATPalomar,
        'neat geodss': schema.NEATMauiGEODSS,
    }

    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)

    def caught(self, queryid):
        """Return results from catch query.


        Parameters
        ----------
        queryid : int
            User's query ID.


        Returns
        -------
        rows : list
            Results as lists of sqlalchemy objects: ``[Caught, Obs,
            Found, Obj]``.

        """

        rows = (self.db.session.query(Caught, Obs, Found, Obj)
                .join(Obs, Caught.obsid == Obs.obsid)
                .join(Found, Caught.foundid == Found.foundid)
                .join(Obj, Found.objid == Obj.objid)
                .filter(Caught.queryid == queryid)
                .all())
        return rows

    def drop(self, queryid):
        """Drop a caught object and query.


        Parameters
        ----------
        queryid : int
            Query ID.


        Returns
        -------
        n : int
            Number of rows deleted.

        """

        rows = (self.db.session.query(Caught, Found)
                .join(Found, Caught.foundid == Found.foundid)
                .filter(Caught.queryid == queryid)
                .all())
        n = 0
        for row in rows:
            # cascading will also delete the Caught rows
            self.db.session.delete(row[1])
            n += 1

        return n

    def query(self, query, source='any', cached=True, **kwargs):
        """Try to catch an object in survey data.


        Parameters
        ----------
        query : str
            User's query string.

        source : string, optional
            Survey source table name.  See ``Catch.surveys.keys()``
            for possible values, or use `'any'` to search each survey.

        cached : bool, optional
            Use cached results, if possible.

        **kwargs
            Any `~sbsearch.sbsearch.find_object` keyword except
            ``save`` or ``update``.


        Returns
        -------
        caught : generator
            Survey matches as lists of sqlalchemy objects:
            ``[Caught, Obs, Found, Obj]``.

        """

        sources = []
        if source == 'any':
            sources = self.SURVEYS.keys()
        else:
            sources = [source]

        for source in sources:
            q = self._check_cache(query, source)
            if q is not None and not cached:
                n = self.drop(q.queryid)
                q = None
                self.logger.info('Dropped {} cached results in "{}"'
                                 .format(n, source))

            if q is None:
                q = CatchQueries(query=str(query),
                                 source=source,
                                 date=Time.now().iso)
                self.db.session.add(q)
                self.db.session.commit()
                self._query(q.queryid, query, source, **kwargs)

            for row in self.caught(q.queryid):
                yield row

    def _check_cache(self, query, source):
        """Has this query been cached?"""
        try:
            q = (self.db.session.query(CatchQueries)
                 .filter(CatchQueries.query == query)
                 .filter(CatchQueries.source == source)
                 .one())
        except NoResultFound:
            q = None
        return q

    def _query(self, queryid, query, source, **kwargs):
        kwargs['save'] = True
        kwargs['update'] = True
        kwargs['source'] = self.SURVEYS[source]
        kwargs['location'] = self.SURVEYS[source].__obscode__

        obsids, foundids, newids = self.find_object(str(query), **kwargs)
        for obsid, foundid in zip(obsids, foundids):
            caught = Caught(
                queryid=queryid,
                obsid=obsid,
                foundid=foundid
            )
            self.db.session.add(caught)
        self.db.session.commit()

        self.logger.info(
            'Query {} caught {} observations of {} in "{}"'
            .format(queryid, len(obsids), query, source))

    def verify_database(self):
        super().verify_database([
            'catch_queries', 'caught',
            'neat_palomar',
            'neat_maui_geodss',
        ])
