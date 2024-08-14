try:
    from .version import version as __version__
except ImportError:
    __version__ = ""

from sbsearch.target import FixedTarget
from .catch import Catch, IntersectionType
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

    # define subparsers
    subparsers = parser.add_subparsers(help="sub-command help")

    verify = subparsers.add_parser(
        "verify", help="connect to database and verify and create tables"
    )
    verify.set_defaults(command="verify")

    list_sources = subparsers.add_parser("sources", help="show available data sources")
    list_sources.set_defaults(command="sources")

    moving = subparsers.add_parser("moving", help="search for a moving object")
    moving.set_defaults(command="moving")

    fixed = subparsers.add_parser("fixed", help="search for a fixed object")
    fixed.set_defaults(command="fixed")

    # subparser arguments
    # moving and fixed have an overlap in parameters
    moving.add_argument("desg", help="object designation")

    fixed.add_argument("ra", help="Right ascension")
    fixed.add_argument("dec", help="Declination")
    fixed.add_argument(
        "--unit",
        default="hourangle,deg",
        help="RA, Dec unit, may be a single string, or two separated"
        " by a comma (default: hourangle,deg)",
    )

    for subparser in (moving, fixed):
        subparser.add_argument(
            "--source",
            dest="sources",
            action="append",
            help="search this observation source (may be used multiple times)",
        )
        subparser.add_argument(
            "--start-date",
            dest="start_date",
            type=Time,
            help="search after this date/time",
        )
        subparser.add_argument(
            "--stop-date",
            dest="stop_date",
            type=Time,
            help="search before this date/time",
        )

    moving.add_argument(
        "--force", dest="cached", action="store_false", help="do not use cached results"
    )

    for subparser in (moving, fixed):
        subparser.add_argument("-o", help="write table to this file")

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
        catch.start_date = args.start_date
        catch.stop_date = args.stop_date

        if args.command == "verify":
            pass
        elif args.command == "sources":
            print("Available sources:\n  *", "\n  * ".join(catch.sources.keys()))
        elif args.command == "moving":
            job_id = uuid.uuid4()
            catch.query(
                args.desg,
                job_id,
                sources=args.sources,
                cached=args.cached,
            )
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
        elif args.command == "fixed":
            job_id = uuid.uuid4()
            target = FixedTarget.from_radec(args.ra, args.dec, unit=args.unit)
            observations = catch.query(
                target,
                job_id,
                sources=args.sources,
            )
            columns = set()
            for obs in observations:
                r = {}
                # Aggregate fields and values from each data object, which may
                # be from different surveys
                for k, v in _serialize_object(obs):
                    r[k] = v

                columns = columns.union(set(r.keys()))

                r["cutout_url"] = obs.cutout_url(target.ra.deg, target.dec.deg)
                r["date"] = Time((obs.mjd_start + obs.mjd_stop) / 2, format="mjd").iso

                rows.append(r)

    if args.command == "moving":
        if rows == []:
            print("# none found")
        else:
            # make sure all rows have all columns
            for i in range(len(rows)):
                for col in columns:
                    rows[i][col] = rows[i].get(col)
            tab = Table(rows=rows)

            # add a column for the target
            tab["designation"] = args.desg

            # re-order columns
            all_colnames = tab.colnames
            base_colnames = [
                "designation",
                "source",
                "date",
                "mjd",
                "ra",
                "dec",
                "dra",
                "ddec",
                "vmag",
                "rh",
                "drh",
                "delta",
                "phase",
                "elong",
                "sangle",
                "vangle",
                "true_anomaly",
                "unc_a",
                "unc_b",
                "unc_theta",
                "retrieved",
                "filter",
                "exposure",
                "mjd_start",
                "mjd_stop",
                "fov",
                "airmass",
                "seeing",
                "maglimit",
                "found_id",
                "object_id",
                "observation_id",
                "orbit_id",
                "query_id",
                "archive_url",
                "cutout_url",
            ]
            colnames = base_colnames + list(set(all_colnames) - set(base_colnames))
            tab = tab[colnames]

            if args.o:
                tab.write(args.o, format="ascii.fixed_width_two_line", overwrite=True)
            else:
                tab.pprint(-1, -1)

    if args.command == "fixed":
        if rows == []:
            print("# none found")
        else:
            # make sure all rows have all columns
            for i in range(len(rows)):
                for col in columns:
                    rows[i][col] = rows[i].get(col)
            tab = Table(rows=rows)

            # add columns for the target in the user's format
            tab["ra"] = args.ra
            tab["dec"] = args.dec

            # re-order columns
            all_colnames = tab.colnames
            base_colnames = [
                "source",
                "date",
                "ra",
                "dec",
                "filter",
                "exposure",
                "mjd_start",
                "mjd_stop",
                "fov",
                "airmass",
                "seeing",
                "maglimit",
                "observation_id",
                "archive_url",
                "cutout_url",
            ]
            colnames = base_colnames + list(set(all_colnames) - set(base_colnames))
            tab = tab[colnames]

            if args.o:
                tab.write(args.o, format="ascii.fixed_width_two_line", overwrite=True)
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
