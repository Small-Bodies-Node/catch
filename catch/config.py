# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Config']

import sbsearch.config


class Config(sbsearch.config.Config):
    """Catch configuration.

    Controls database location, and log file location.

    Parameters
    ----------
    **kwargs
      Additional or updated configuration parameters and values.

    """

    DEFAULT_FILE = './.catch.config'
