import argparse
from sqlalchemy.orm.session import make_transient
from catch import Catch, Config
from catch.model import NEATMauiGEODSS, NEATPalomarTricam, SkyMapper
from sbsearch.model import ObservationSpatialTerm
from sbsearch.logging import ProgressBar

parser = argparse.ArgumentParser()
parser.add_argument('source_config')
parser.add_argument('destination_config')
args = parser.parse_args()

src = Catch.with_config(Config.from_file(args.source_config))
dest = Catch.with_config(Config.from_file(args.destination_config))

# for this example, just copy the observation tables and the spatial index
for table in (NEATMauiGEODSS, NEATPalomarTricam, SkyMapper,
              ObservationSpatialTerm):
    n_obs = src.db.session.query(table).count()
    with ProgressBar(n_obs, src.logger, scale='log') as bar:
        n_obs = 0
        while True:
            rows = (
                src.db.session.query(table)
                .offset(n_obs)
                .limit(1000)
                .all()
            )
            if len(rows) == 0:
                break

            for row in rows:
                n_obs += 1
                bar.update()
                src.db.session.expunge(row)
                make_transient(row)
                dest.db.session.add(row)

            # check-in to avoid soaking up too much memory
            dest.db.session.commit()

    dest.db.session.commit()
