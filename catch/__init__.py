try:
    from .version import version as __version__
except ImportError:
    __version__ = ""

from .catch import Catch, IntersectionType  # noqa: F401
from . import stats  # noqa: F401
from .config import Config  # noqa: F401
