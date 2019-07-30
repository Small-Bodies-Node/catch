# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import os
import uuid
from warnings import warn
from collections import OrderedDict

import numpy as np
from astropy.nddata import CCDData
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import Angle
import astropy.units as u

from sbsearch import SBSearch
from .config import Config
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
    sessionid : str, optional
        UUID4 formatted session ID string.  If not provided, one will
        be generated.

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
        self._validate_sessionid()

    def caught(self, queryid):
        """Return results from catch query.

        Parameters
        ----------
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
                .filter(CatchQueries.sessionid == self.config['sessionid'])
                .filter(CatchQueries.queryid == int(queryid))
                .all())
        return rows

    def query(self, query, source='any', **kwargs):
        """Try to catch an object in survey data.

        Parameters
        ----------
        query : str
            User's query string.

        source : string, optional
            Survey source table name.  See ``Catch.surveys.keys()``
            for possible values, or use `'any'` to search each survey.

        **kwargs
            Any `~sbsearch.sbsearch.find_object` keyword except
            ``save`` or ``update``.

        Returns
        -------
        queryid : int
            Unique database ID for this query.

        """

        q = CatchQueries(
            sessionid=self.config['sessionid'],
            query=str(query)
        )
        self.db.session.add(q)
        self.db.session.commit()

        sources = []
        if source == 'any':
            sources = self.SURVEYS.keys()
        else:
            sources = [source]

        for source in sources:
            self._query(q.queryid, query, source, **kwargs)

        return q.queryid

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
            'Query {} for session {} caught {} observations of {} in "{}"'
            .format(queryid, self.config['sessionid'], len(obsids), query,
                    source))

    def cutouts(self, queryid, force=False):
        """Generate cutouts based on caught data.

        This functionality needs to be moved elsewhere and removed
        from catch.

        Parameters
        ----------
        queryid : int
            User's query ID.

        force : bool, optional
            If target files exist, they will be overwritten.

        """

        warn(DeprecationWarning('cutouts is slated for removal'))

        self.logger.info('Creating cutouts.')

        queryid = int(queryid)
        size = u.Quantity(self.config['cutout size'])
        n = 0
        total = 0
        for row in self.caught(queryid):
            total += 1
            
            path = [
                self.config['archive path'],
                row.Obs.__product_path__,
            ] + row.Obs.productid.lower().split('_')

            inf = os.path.join(*path) + '.fit.fz'
            outf = os.path.join(self.config['cutout path'],
                                self.config['sessionid'], str(queryid),
                                row.Obs.productid + '_cutout.fits')
            self.logger.debug(inf + ' → ' + outf)

            if os.path.exists(outf) and not force:
                continue

            # two directories may need to be created, the session id and
            # query id; the rest should be created ahead of time
            p = os.path.dirname(outf).split('/')
            for i in (len(p) - 1, len(p)):
                d = '/'.join(p[:i])
                if not os.path.exists(d):
                    os.mkdir(d)
                if not os.path.isdir(d):
                    raise ValueError(
                        ('{} is not a directory, verify cutout directory'
                         ' integrity.').format(d))

            im, h = fits.getdata(inf, header=True)
            # header is crashing WCS, so generate one "manually"
            wcs = WCS(naxis=2)
            wcs.wcs.ctype = h['CTYPE1'], h['CTYPE2']
            wcs.wcs.crval = h['CRVAL1'], h['CRVAL2']
            wcs.wcs.crpix = h['CRPIX1'], h['CRPIX2']
            wcs.wcs.cdelt = h['CDELT1'], h['CDELT2']
            for k in ['CTYPE1', 'CTYPE2', 'CRVAL1', 'CRVAL2',
                      'CRPIX1', 'CRPIX2', 'CDELT1', 'CDELT2']:
                del h[k]
            ccd = CCDData(im, meta=h, wcs=wcs, unit='adu')

            ra = Angle(row.Found.ra, 'deg')
            dec = Angle(row.Found.dec, 'deg')
            x0, y0 = wcs.all_world2pix([[ra.deg, dec.deg]], 0)[0]

            corners = Angle([
                [ra - size, dec - size],
                [ra + size, dec - size],
                [ra + size, dec + size],
                [ra - size, dec + size]
            ]).deg

            x, y = wcs.all_world2pix(corners, 0).T.astype(int)

            s = np.s_[max(0, y.min()):min(y.max(), im.shape[0]),
                      max(0, x.min()):min(x.max(), im.shape[1])]
            cutout = ccd[s]

            updates = OrderedDict()
            updates['desg'] = (
                row.Obj.desg, 'Target designation'
            )
            updates['jd'] = (
                row.Found.jd, 'Observation mid-time'
            )
            updates['rh'] = (
                row.Found.rh, 'Heliocentric distance, au'
            )
            updates['delta'] = (
                row.Found.delta, 'Observer-target distance, au'
            )
            updates['phase'] = (
                row.Found.phase, 'Sun-target-observer angle, deg'
            )
            updates['rdot'] = (
                row.Found.rdot, 'Heliocentric radial velocity, km/s'
            )
            updates['selong'] = (
                row.Found.selong, 'Solar elongation, deg'
            )
            updates['sangle'] = (
                row.Found.sangle,
                'Projected target->Sun position angle, deg'
            )
            updates['vangle'] = (
                row.Found.vangle,
                'Projected velocity position angle, deg'
            )
            updates['trueanom'] = (
                row.Found.trueanomaly, 'True anomaly (osculating), deg'
            )
            updates['tmtp'] = (
                row.Found.tmtp, 'T-Tp (osculating), days'
            )
            updates['tgtra'] = (
                row.Found.ra, 'Target RA, deg'
            )
            updates['tgtdec'] = (
                row.Found.dec, 'Target Dec, deg'
            )
            updates['tgtdra'] = (
                row.Found.dra, 'RA*cos(dec) rate of change, arcsec/s'
            )
            updates['tgtddec'] = (
                row.Found.ddec, 'Dec rate of change, arcsec/s'
            )
            updates['unc_a'] = (
                row.Found.unc_a, 'Error ellipse semi-major axis, arcsec'
            )
            updates['unc_b'] = (
                row.Found.unc_b, 'Error ellipse semi-minor axis, arcsec'
            )
            updates['unc_th'] = (
                row.Found.unc_theta, 'Error ellipse position angle, deg'
            )
            updates['vmag'] = (
                row.Found.vmag, 'predicted brightness (magnitude)'
            )
            cutout.meta.update(updates)
            cutout.write(outf, overwrite=True)

            n += 1

        self.logger.info('Cutout {} files, {} already existed.'
                         .format(n, total - n))

    def _validate_sessionid(self):
        """Validates the session ID."""
        if self.config['sessionid'] is None:
            # generate one
            self.config.config['sessionid'] = str(uuid.uuid4())
        else:
            try:
                sess = uuid.UUID(self.config['sessionid'])
            except ValueError:
                raise InvalidSessionID()

            if sess.version != 4:
                raise InvalidSessionID()


    def verify_database(self):
        super().verify_database([
            'catch_queries', 'caught',
            'neat_palomar',
            'neat_maui_geodss',
         ])
