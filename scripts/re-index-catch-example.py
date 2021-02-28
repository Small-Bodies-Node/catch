from catch import Catch, Config

config = Config.from_file('catch_dev.config')
with Catch.with_config(config) as c:
    c.re_index()
