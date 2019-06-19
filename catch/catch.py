# Licensed with the 3-clause BSD license.  See LICENSE for details.

__all__ = ['Catch']

from sbsearch import SBSearch
from .config import Config


class Catch(SBSearch):
    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        kwargs['location'] = 'I41'
        self.config = Config(**kwargs) if config is None else config
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)
