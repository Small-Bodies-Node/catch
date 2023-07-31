"""

CSS daily harvest has been updating catch_dev, but now we want to update catch_prod.

"""

import argparse
from sqlalchemy.orm.session import make_transient

from catch import Catch, Config
from catch.model import CatalinaBigelow, CatalinaLemmon, Observation
from sbsearch.logging import ProgressPercent

parser = argparse.ArgumentParser()
parser.add_argument('source', help='catch configuration file for source database')
parser.add_argument('destination', help='catch configuration file for destination database')
args = parser.parse_args()

src = Catch.with_config(Config.from_file(args.source))
dest = Catch.with_config(Config.from_file(args.destination))

# I dropped this and a few other indices with psql instead
# dest.db.drop_spatial_index()

# my original version used query(table).order_by(table.source_id).limit(1000).offset(offset)
# but this is slow for large offsets
#
# instead, use the source_id and index and paginate without offsets

chunk = 10_000

for table, last_source_id in zip((CatalinaLemmon, CatalinaBigelow), (0, 0)):
    n_obs = src.db.session.execute(f"SELECT COUNT(source_id) FROM {table.__tablename__} WHERE source_id > {last_source_id}").scalar()
    print(table.__tablename__, n_obs, last_source_id)

    with ProgressPercent(n_obs, src.logger) as progress_widget:
        while True:
            # find last source added, and get next 10,000
            rows = (
                src.db.session.query(table)
                .filter(table.source_id > last_source_id)
                .order_by(table.source_id)  # must have an order!
                .limit(chunk)
                .all()
            )
            if len(rows) == 0:
                break

            for row in rows:
                # print(row.source, row.source_id, row.product_id)
                last_source_id = row.source_id

                src.db.session.expunge(row)
                make_transient(row)
                row.observation_id = None
                row.source_id = None
                dest.db.session.add(row)

            # check-in to avoid soaking up too much memory
            dest.db.session.commit()

            progress_widget.update(len(rows))

    dest.db.session.commit()

# src.logger.info("re-create spatial index")
# dest.db.create_spatial_index()
print("you need to manually recreate spatial index")

src.db.session.close()
dest.db.session.close()
