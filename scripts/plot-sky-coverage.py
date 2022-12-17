import argparse
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
from catch import Catch, Config
from catch.model import Observation, ExampleSurvey


def get_fields_of_view(survey):
    print('WARNING: using quick fix')
    config = Config.from_file('/elatus3/catch/catch-apis-v2/catch_dev.config')
    with Catch.with_config(config) as c:
        query = c.db.session.query(Observation.fov)
        if survey is not None:
            query = query.filter(Observation.source == survey)
        query = query.yield_per(1000)

        for fov, in query:
            # quick fix
            i = fov.index(':')
            j = fov.index(',')
            ra = float(fov[:i])
            dec = float(fov[(i + 1):j])
            yield ra, dec


def make_sky_coverage_map(survey, density):
    """density is number of bins per deg at the equator"""
    ra_bins = np.linspace(0, 360, density * 360 + 1)
    dec_bins = np.linspace(-90, 90, density * 180 + 1)
    cov = np.zeros((density * 180, density * 360))
    for (ra, dec) in get_fields_of_view(survey):
        i = np.digitize(ra, ra_bins) - 1
        j = np.digitize(dec, dec_bins) - 1
        try:
            cov[j, i] += 1
        except:
            breakpoint()

    # roll from 0 to 360 to -180 to 180
    cov = np.roll(cov, cov.shape[1] // 2, axis=1)
    # place E on the right
    cov = cov[:, ::-1]
    return cov


def plot(cov):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(num=1, clear=True, subplot_kw=dict(projection="aitoff"))
    ra, dec = np.meshgrid(
        np.linspace(-np.pi, np.pi, cov.shape[1] + 1),
        np.linspace(-np.pi/2, np.pi/2, cov.shape[0] + 1)
    )
    im = ax.pcolormesh(ra, dec, np.log10(cov), shading="flat", cmap='magma')
    plt.setp(ax, xticklabels=[], yticklabels=[])
    cb = fig.colorbar(im, ax=ax, orientation='horizontal')
    cb.set_label('$\log_{10}$(N$_{images}$)')
    # cb.minorticks_on()


if __name__ == '__main__':
    sources = [source.__tablename__ for source in Observation.__subclasses__()
               if source is not ExampleSurvey]
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--survey', choices=sources,
                        help='plot this data source')
    parser.add_argument('--density', type=int, default=1,
                        help=('Number of bins per degree at the equator, default is 20'
                              ' for 3 arcmin resolution'))
    parser.add_argument('-f', help="plot this saved sky coverage map")
    parser.add_argument('-o', help='output file name prefix')
    parser.add_argument('--format', default='png', help='plot file format')
    parser.add_argument('--dpi', type=int, default=200)
    args = parser.parse_args()

    if args.o is None:
        if args.survey is None:
            args.o = 'all'
        else:
            args.o = args.survey

    if args.f:
        cov = fits.getdata(args.f)
    else:
        cov = make_sky_coverage_map(args.survey, args.density)
        fits.writeto('.'.join((args.o, 'fits')), cov, overwrite=True)

    plot(cov)
    plt.savefig('.'.join((args.o, args.format)), dpi=args.dpi)
