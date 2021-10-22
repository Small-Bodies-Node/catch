
try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

from .catch import *
from .config import *
from . import model
