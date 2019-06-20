# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

from sbsearch import SBSearch
from .config import Config
from .schema import CatchQueries, Caught, Obs, Found, Obj


class Catch(SBSearch):
    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        kwargs['location'] = 'I41'
        self.config = Config(**kwargs) if config is None else config
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)

    def caught(self, sessionid, queryid):
        """Return results from catch query.

        Parameters
        ----------
        sessionid : str
            User's session ID.

        queryid : int
            User's query ID.

        Returns
        -------
        rows : list
            Results as lists of sqlalchemy objects: ``[CatchQueries,
            Caught, Obs, Found, Obj]``.

        """

        rows = (self.db.session.query(Caught, CatchQueries, Obs, Found, Obj)
                .join(CatchQueries, Caught.queryid == CatchQueries.queryid)
                .join(Obs, Caught.obsid == Obs.obsid)
                .join(Found, Caught.foundid == Found.foundid)
                .join(Obj, Found.objid == Obj.objid)
                .filter(CatchQueries.sessionid == sessionid)
                .filter(CatchQueries.queryid == int(queryid))
                .all())
        return rows

    def query(self, sessionid, query, **kwargs):
        """Try to catch an object in all survey data.

        Parameters
        ----------
        sessionid : str
            User's session ID.

        query : str
            User's query string.

        **kwargs
            Any `~sbsearch.sbsearch.find_object` keyword except
            ``save`` or ``update``.

        Returns
        -------
        queryid : int
            Unique database ID for this query.

        """

        q = CatchQueries(
            sessionid=str(sessionid),
            query=str(query)
        )
        self.db.session.add(q)
        self.db.session.commit()

        kwargs['save'] = True
        kwargs['update'] = True
        obsids, foundids, newids = self.find_object(str(query), **kwargs)
        for obsid, foundid in zip(obsids, foundids):
            caught = Caught(
                queryid=q.queryid,
                obsid=obsid,
                foundid=foundid
            )
            self.db.session.add(caught)
        self.db.session.commit()

        self.logger.info(
            'Query {} for session {} caught {} observations of {}'
            .format(q.queryid, sessionid, len(obsids), query))

        return q.queryid

    def verify_database(self):
        super().verify_database(['catch_queries', 'neat_palomar', 'caught'])
