# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Config']

import os
import uuid
import sbsearch.config

_config_example = '''
{
  "database": "postgresql://user:password@host/database",
  "log": "/path/to/catch.log",
  "cutout size": "5arcmin",
  "archive path": "/path/to/archive/directory",
  "cutout path": "/path/to/cutout/directory",
  "sessionid": "valid catch session id"
}
'''


class Config(sbsearch.config.Config):
    """Catch configuration.

    Controls database location, and log file location.

    Parameters
    ----------
    **kwargs
        Additional or updated configuration parameters and values.

    """

    DEFAULT_FILES = ['catch.config', '.catch.config',
                     os.path.expanduser('~/.config/catch.config')]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        uuid.UUID(self['sessionid'])  # validate
