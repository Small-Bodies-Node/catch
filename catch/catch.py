# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

import os
import uuid
from collections import OrderedDict
import numpy as np
from astropy.nddata import CCDData
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import Angle
import astropy.units as u
from sbsearch import SBSearch
from .config import Config
from .schema import CatchQueries, Caught, Obs, Found, Obj


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

    def __init__(self, config=None, sessionid=None, save_log=False,
                 disable_log=False, **kwargs):
        if sessionid is None:
            self.sessionid = uuid.uuid4()
        else:
            self.sessionid = str(uuid.UUID(sessionid))

        kwargs['location'] = 'I41'
        self.config = Config(**kwargs) if config is None else config
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)

    def caught(self, queryid):
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
                .filter(CatchQueries.sessionid == self.sessionid)
                .filter(CatchQueries.queryid == int(queryid))
                .all())
        return rows

    def query(self, query, **kwargs):
        """Try to catch an object in all survey data.

        Parameters
        ----------
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
            sessionid=self.sessionid,
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
            .format(q.queryid, self.sessionid, len(obsids), query))

        return q.queryid

    def cutouts(self, queryid, update=False):
        """Generate cutouts based on caught data.

        Parameters
        ----------
        queryid : int
            User's query ID.

        update : bool, optional
            If target files exist, they will be overwritten.

        """

        self.logger.info('Creating cutouts.')

        queryid = int(queryid)
        size = u.Quantity(self.config['cutout size'])
        n = 0
        total = 0
        for row in self.caught(queryid):
            total += 1
            path = [self.config['archive path'], 'neat',
                    'tricam', 'data']
            path += row.Obs.productid.lower().split('_')
            inf = os.path.join(*path) + '.fit.fz'
            outf = os.path.join(self.config['cutout path'],
                                'neat', self.sessionid,
                                str(queryid),
                                row.Obs.productid + '_cutout.fits')
            self.logger.debug(inf + ' → ' + outf)

            if os.path.exists(outf) and not update:
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

    def verify_database(self):
        super().verify_database(['catch_queries', 'neat_palomar', 'caught'])
