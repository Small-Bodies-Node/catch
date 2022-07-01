import argparse
import numpy as np
import geoalchemy2.functions as func
import matplotlib.pyplot as plt
from astropy.coordinates import spherical_to_cartesian
import healpy as hp
import catch


def get_fields_of_view(survey):
    config = catch.Config.from_file('/elatus3/catch/catch.config')
    table = catch.Catch.SOURCES.get(survey, catch.schema.Obs)
    with catch.Catch(config) as c:
        query = (c.db.session.query(func.ST_AsText(catch.schema.Obs.fov))
                 .yield_per(1000))
        for fov, in query:
            coords = np.radians(np.array(
                fov[fov.rfind('(') + 1:fov.find(')')]
                .replace(' ', ',').split(','),
                float).reshape((5, 2))[:4])
            xyz = spherical_to_cartesian(1, coords[:, 1], coords[:, 0])
            yield np.array(xyz).T


def make_sky_coverage_map(survey, nside):
    cov = np.zeros(hp.nside2npix(nside), dtype=int)
    for coords in get_fields_of_view(survey):
        cov[hp.query_polygon(nside, coords)] += 1

    return cov


def plot(cov):
    plt.style.use('dark_background')
    fig = plt.figure(1)
    plt.clf()
    hp.mollview(np.log10(cov), fig=1, bgcolor='k',
                cmap='magma', title='CATCH Sky Coverage', )
    fig.axes[1].set_title('$\log_{10}$(N$_{images}$)')
    fig.axes[1].minorticks_on()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--survey', choices=catch.Catch.SOURCES,
                        help='limit analysis to this data source')
    parser.add_argument('--nside', default=2048,
                        help=('Healpix nside parameter, default is 2048'
                              ' for 1.7 arcmin resolution'))
    parser.add_argument('-o', default='sky-coverage.png',
                        help='output file name prefix')
    parser.add_argument('--format', default='png', help='plot file format')
    parser.add_argument('--dpi', type=int, default=200)
    args = parser.parse_args()

    cov = make_sky_coverage_map(args.survey, args.nside)
    hp.write_map('.'.join((args.o, 'fits')), cov)

    plot(cov)
    plt.savefig('.'.join((args.o, args.format)), dpi=args.dpi)
