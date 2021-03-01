import argparse
from catch import Catch, Config

parser = argparse.ArgumentParser()
parser.add_argument('config')
args = parser.parse_args()

config = Config.from_file(args.config)
with Catch.with_config(config) as c:
    c.re_index()
