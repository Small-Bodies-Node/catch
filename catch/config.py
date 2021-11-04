# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Config']

import os
from typing import Dict, List, Union
import sbsearch.config

_config_example = '''
{
  "database": "postgresql://user:password@host/database",
  "log": "/path/to/catch.log",
  "debug": false
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

    DEFAULT_FILES: List[str] = [
        'catch.config',
        '.catch.config',
        os.path.expanduser('~/.config/catch.config')
    ]

    DEFAULT_PARAMETERS: Dict[str, Union[str, float, int, bool]] = {
        "database": "postgresql://user@host/database",
        "log": "/dev/null",
        "debug": False
    }
