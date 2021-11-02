import argparse
import numpy as np
import matplotlib.pyplot as plt
from astropy.coordinates import spherical_to_cartesian
import healpy as hp
from catch import Catch, Config

"""
Examples:
python3 plot-sky-coverage.py --config=/elatus3/catch/catch-apis-v2/catch_dev.config --source=skymapper
python3 plot-sky-coverage.py --config=/elatus3/catch/catch-apis-v2/catch_dev.config --source=neat_palomar_tricam
python3 plot-sky-coverage.py --config=/elatus3/catch/catch-apis-v2/catch_dev.config --source=neat_maui_geodss
"""


def get_fields_of_view(catch):
    query = (catch.db.session.query(catch.source.fov)
             .yield_per(10000))
    for fov, in query:
        ra, dec = np.radians(np.array([c.split(':')
                                       for c in fov.split(',')],
                                      float)).T
        xyz = spherical_to_cartesian(1, dec, ra)
        yield np.array(xyz).T


def make_sky_coverage_map(catch, nside):
    cov = np.zeros(hp.nside2npix(nside), dtype=int)
    for coords in get_fields_of_view(catch):
        cov[hp.query_polygon(nside, coords)] += 1

    return cov


def plot(cov, source_name):
    plt.style.use('dark_background')
    fig = plt.figure(1)
    plt.clf()
    hp.mollview(np.log10(cov), fig=1, bgcolor='k',
                cmap='magma', title=source_name)
    fig.axes[1].set_title('$\log_{10}$(N$_{images}$)')
    fig.axes[1].minorticks_on()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='CATCH configuration file.')
    parser.add_argument('--source', default='observation',
                        help=('limit analysis to this data source '
                              '(default: show all data)'))
    parser.add_argument('--nside', default=2048,
                        help=('Healpix nside parameter, default is 2048'
                              ' for 1.7 arcmin resolution'))
    parser.add_argument('-o', default=None,
                        help='output file name prefix, default based on --source')
    parser.add_argument('--format', default='png', help='plot file format')
    parser.add_argument('--dpi', type=int, default=200)
    args = parser.parse_args()

    prefix = args.source if args.o is None else args.o

    config = Config.from_file(args.config)
    with Catch.with_config(config) as catch:
        catch.source = args.source
        source_name = catch.source.__data_source_name__
        cov = make_sky_coverage_map(catch, args.nside)

    hp.write_map('.'.join((prefix, 'fits')), cov, overwrite=True)
    plot(cov, source_name)
    plt.savefig('.'.join((prefix, args.format)), dpi=args.dpi)
