# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import sys
import uuid
import logging

from astropy.time import Time
from sbsearch import SBSearch
from sqlalchemy.orm import with_polymorphic

from . import schema
from .schema import CatchQueries, Caught, Obs, Found, Obj


class CatchException(Exception):
    pass


class InvalidSessionID(CatchException):
    pass


class InvalidSourceName(CatchException):
    pass


class FindObjectFailure(CatchException):
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
        'skymapper': schema.SkyMapper,
    }

    VMAX = 27

    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)
        self.logger.debug('Initialized Catch')

    def caught(self, job_id, vmax=None):
        """Return results from catch query.


        Parameters
        ----------
        job_id : uuid.UUID or string
            Unique job ID for original query.  UUID version 4.

        vmax : float, optional
            Maximum magnitude for returned results.


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

        if vmax is not None:
            rows = rows.filter(Found.vmag <= vmax)

        return rows

    def drop(self, queryid):
        """Drop a caught query.


        Parameters
        ----------
        queryid : int
            Query ID.


        Returns
        -------
        None

        """

        query = (self.db.session.query(CatchQueries)
                 .filter(CatchQueries.queryid == queryid)
                 .all())

        # cascades should delete caught rows and found objects
        self.db.session.delete(query)
        self.db.session.commit()

    def query(self, target, job_id, sources=None, cached=True, **kwargs):
        """Try to catch an object in survey data.

        Publishes messages to the Python logging system under the name
        'CATCH-APIs <job_id>'.


        Parameters
        ----------
        target : string
            Target for which to search.

        job_id : uuid.UUID or string
            Unique ID for this query.  UUID version 4.

        sources : list of strings, optional
            Limit search to these sources.  See ``Catch.SOURCES.keys()``
            for possible values.

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

        _sources = self._validate_sources(sources)
        job_id = uuid.UUID(str(job_id), version=4)

        # logger for CATCH-APIs messages
        task_messenger = logging.getLogger(
            'CATCH-APIs {}'.format(job_id.hex))
        task_messenger.setLevel(logging.INFO)
        if len(task_messenger.handlers) == 0:
            # always log to the console
            formatter = logging.Formatter(
                '%(levelname)s ({}): %(message)s'.format(job_id.hex))
            console = logging.StreamHandler(sys.stdout)
            console.setFormatter(formatter)
            task_messenger.addHandler(console)

        count = 0
        for source in _sources:
            self.logger.debug('Query {}'.format(source))
            source_name = self.SOURCES[source].__data_source_name__

            cached_query = self._find_catch_query(target, source)

            q = CatchQueries(query=str(target),
                             jobid=job_id.hex,
                             source=source,
                             date=Time.now().iso,
                             status='in progress')
            self.db.session.add(q)
            self.db.session.commit()
            if cached and cached_query is not None:
                n = self._add_cached_results(q, cached_query)
                count += n
                task_messenger.info('Added {} cached results from {}.'
                                    .format(n, source_name))
                q.status = 'finished'
                self.db.session.commit()
            else:
                try:
                    n = self._query(q, target, source, **kwargs)
                except FindObjectFailure as e:
                    q.status = 'errored'
                    task_messenger.error(str(e))
                    self.logger.error(e)
                    raise
                else:
                    count += n
                    task_messenger.info('Caught {} observations in {}.'
                                        .format(n, source_name))
                    q.status = 'finished'
                finally:
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

    @staticmethod
    def skymapper_cutout_url(found, obs, size=0.0833, format='fits'):
        """Return SkyMapper cutout URL.

        http://skymapper.anu.edu.au/how-to-access/#public_siap

        For example:
            http://api.skymapper.nci.org.au/public/siap/dr2/get_image?IMAGE=20140425124821-10&SIZE=0.0833&POS=189.99763,-11.62305&FORMAT=fits

        size in deg

        format = fits, png, or mask

        """

        return (
            'http://api.skymapper.nci.org.au/public/siap/dr2/get_image?'
            f'IMAGE={obs.productid}&SIZE={size}&POS={found.ra},{found.dec}&FORMAT={format}'
        )

    def _find_catch_query(self, target, source):
        """Find query ID for this target and source.

        Assumes the last search with status=='finished' is the most relevant.

        """
        q = (self.db.session.query(CatchQueries)
             .filter(CatchQueries.query == target)
             .filter(CatchQueries.source == source)
             .filter(CatchQueries.status == 'finished')
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
        kwargs['vmax'] = kwargs.get('vmax', self.VMAX)

        try:
            obsids, foundids, newids = self.find_object(
                str(target), **kwargs)
        except Exception as e:
            if self.debug:
                # raise original exception
                raise
            else:
                raise FindObjectFailure(str(e))

        for foundid in foundids:
            caught = Caught(
                queryid=query.queryid,
                foundid=foundid
            )
            self.db.session.add(caught)

        self.db.session.commit()
        self.logger.debug('query Added caught objects')
        return len(foundids)

    def _validate_sources(self, sources):
        if sources is None:
            return self.SOURCES.keys()

        invalid_sources = set(sources) - set(self.SOURCES.keys())
        if len(invalid_sources) > 0:
            raise InvalidSourceName(invalid_sources)

        return sources

    def verify_database(self):
        super().verify_database([
            'catch_queries', 'caught',
            'neat_palomar',
            'neat_maui_geodss',
            'skymapper'
        ])
