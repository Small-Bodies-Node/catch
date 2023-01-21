try:
    from .version import version as __version__
except ImportError:
    __version__ = ""

from .catch import *
from .config import *


def catch_cli(*args):
    """CATCH command-line script."""
    import sys
    import argparse
    import uuid
    from astropy.time import Time
    from astropy.table import Table
    from catch import Catch, Config
    from catch.config import _config_example

    parser = argparse.ArgumentParser(
        "catch", epilog=f"Configuration files are JSON-formatted:\n{_config_example}"
    )
    parser.add_argument("--config", help="CATCH configuration file")
    parser.add_argument("--database", help="use this database URI")
    parser.add_argument("--log", help="save log messages to this file")
    parser.add_argument(
        "--arc-limit", type=float, help="maximal arc length to search, radians"
    )
    parser.add_argument(
        "--time-limit", type=float, help="maximal time length to search, days"
    )
    parser.add_argument("--debug", action="store_true", help="debug mode")
    subparsers = parser.add_subparsers(help="sub-command help")

    verify = subparsers.add_parser(
        "verify", help="connect to database and verify and create tables"
    )
    verify.set_defaults(command="verify")

    list_sources = subparsers.add_parser(
        "sources", help="show available data sources")
    list_sources.set_defaults(command="sources")

    search = subparsers.add_parser("search", help="search for an object")
    search.set_defaults(command="search")
    search.add_argument("desg", help="object designation")
    search.add_argument(
        "--source",
        dest="sources",
        action="append",
        help="search this observation source (may be used multiple times)",
    )
    search.add_argument(
        "--force", dest="cached", action="store_false", help="do not use cached results"
    )
    search.add_argument("-o", help="write table to this file")

    args = parser.parse_args()

    try:
        getattr(args, "command")
    except AttributeError:
        parser.print_help()
        sys.exit()

    if args.command == "verify":
        print("Verify databases and create as needed.\n")

    rows = []
    config = Config.from_args(args)
    with Catch.with_config(config) as catch:
        if args.command == "verify":
            pass
        elif args.command == "sources":
            print("Available sources:\n  *",
                  "\n  * ".join(catch.sources.keys()))
        elif args.command == "search":
            job_id = uuid.uuid4()
            catch.query(args.desg, job_id, sources=args.sources,
                        cached=args.cached)
            columns = set()
            # catch.caught returns a list of rows.
            for row in catch.caught(job_id):
                r = {}
                # Each row consists of a Found and an Observation object.  The
                # Observation object will be a subclass, e.g.,
                # NeatPalomarTricam, or SkyMapper.
                for data_object in row:
                    # Aggregate fields and values from each data object
                    for k, v in _serialize_object(data_object):
                        r[k] = v

                columns = columns.union(set(r.keys()))

                r["cutout_url"] = row.Observation.cutout_url(
                    row.Found.ra, row.Found.dec
                )

                r["date"] = Time(row.Found.mjd, format="mjd").iso

                rows.append(r)

    if args.command == "search":
        if rows == []:
            print("# none found")
        else:
            # make sure all rows have all columns
            for i in range(len(rows)):
                for col in columns:
                    rows[i][col] = rows[i].get(col)
            tab = Table(rows=rows)

            # add a column for the target
            tab['designation'] = args.desg

            # re-order columns
            all_colnames = tab.colnames
            base_colnames = ['designation', 'source', 'date', 'mjd', 'ra', 'dec', 'dra', 'ddec', 'vmag', 'rh', 'drh', 'delta', 'phase', 'elong', 'sangle', 'vangle', 'true_anomaly', 'unc_a', 'unc_b', 'unc_theta',
                             'retrieved', 'filter', 'exposure', 'mjd_start', 'mjd_stop', 'fov', 'airmass', 'seeing', 'maglimit', 'found_id', 'object_id', 'observation_id', 'orbit_id', 'query_id', 'archive_url', 'cutout_url']
            colnames = (base_colnames +
                        list(set(all_colnames) - set(base_colnames)))
            tab = tab[colnames]

            if args.o:
                tab.write(args.o, format="ascii.fixed_width_two_line",
                          overwrite=True)
            else:
                tab.pprint(-1, -1)


def _serialize_object(data_object):
    """Iterator over field names to be serialized."""
    from .model import Observation

    common_fields = dir(Observation) + ["archive_url"]

    SKIP_COLUMNS = [
        "spatial_terms",
        "metadata",
        "cutout_url",
        "preview_url",
        "set_fov",
        "registry",
    ]

    # Scan each data object for field names and their values
    for k in dir(data_object):
        # Skip unwanted field names.
        if k.startswith("_") or k in SKIP_COLUMNS:
            continue

        field_name = k  # default is to use the attribute name
        field_value = getattr(data_object, k)

        # Are there are any survey-specific fields to add?
        if hasattr(data_object, "__field_prefix__"):
            if k not in common_fields:
                # This field_name is not in Observation, so must be survey-specific.
                field_name = f"{data_object.__field_prefix__}:{k}"

        yield field_name, field_value
