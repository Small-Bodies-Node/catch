
try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

from .catch import *
from .config import *
from . import model


def catch_cli(*args):
    """CATCH command-line script."""
    import sys
    import argparse
    import uuid
    from astropy.table import Table
    from catch import Catch, Config
    from catch.config import _config_example

    parser = argparse.ArgumentParser(
        'catch',
        epilog=f'Configuration files are JSON-formatted:\n{_config_example}'
    )
    parser.add_argument('--config', help='CATCH configuration file')
    parser.add_argument('--database', help='use this database URI')
    parser.add_argument('--log', help='save log messages to this file')
    parser.add_argument('--debug', action='store_true', help='debug mode')
    subparsers = parser.add_subparsers(help='sub-command help')

    verify = subparsers.add_parser(
        'verify', help='connect to database and verify and create tables')
    verify.set_defaults(command='verify')

    list_sources = subparsers.add_parser(
        'sources', help='show available data sources')
    list_sources.set_defaults(command='sources')

    search = subparsers.add_parser('search', help='search for an object')
    search.set_defaults(command='search')
    search.add_argument('desg', help='object designation')
    search.add_argument(
        '--source', dest='sources', action='append',
        help='search this observation source (may be used multiple times)')
    search.add_argument('--force', dest='cached', action='store_false',
                        help='do not use cached results')

    args = parser.parse_args()

    try:
        getattr(args, 'command')
    except AttributeError:
        parser.print_help()
        sys.exit()

    if args.command == 'verify':
        print('Verify databases and create as needed.\n')

    SKIP_COLUMNS = ['terms', 'metadata',
                    'cutout_url', 'preview_url', 'set_fov']

    rows = []
    config = Config.from_args(args)
    with Catch.with_config(config) as catch:
        if args.command == 'verify':
            pass
        elif args.command == 'sources':
            print('Available sources:\n  *',
                  '\n  * '.join(catch.sources.keys()))
        elif args.command == 'search':
            job_id = uuid.uuid4()
            count = catch.query(args.desg, job_id, sources=args.sources,
                                cached=args.cached, debug=args.debug)
            columns = set()
            for row in catch.caught(job_id):
                r = {}
                for table in row:
                    for k in dir(type(table)):
                        if (k.startswith('_') or k in SKIP_COLUMNS):
                            continue
                        r[k] = getattr(table, k)
                columns = columns.union(set(r.keys()))

                r['url'] = row.Observation.cutout_url(
                    row.Found.ra, row.Found.dec)

                rows.append(r)

    if args.command == 'search':
        if rows == []:
            print('# none found')
        else:
            # make sure all rows have all columns
            for i in range(len(rows)):
                for col in columns:
                    rows[i][col] = rows[i].get(col)
            tab = Table(rows=rows)
            tab.pprint(-1, -1)
