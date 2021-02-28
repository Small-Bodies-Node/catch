# Licensed with the 3-clause BSD license.  See LICENSE for details.

import uuid
import logging


class TaskMessenger:
    """Messenger / logger for CATCH-APIs."""

    def __init__(self, job_id: uuid.UUID, debug: bool = False) -> None:
        job_hex: str = uuid.UUID(str(job_id), version=4).hex
        self.logger: logging.Logger = logging.getLogger(
            'CATCH-APIs {}'.format(job_hex))
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        if len(self.logger.handlers) == 0:
            # always log to the console
            formatter: logging.Formatter = logging.Formatter(
                '%(levelname)s %(asctime)s ({}): %(message)s'.format(job_hex))
            console: logging.StreamHandler = logging.StreamHandler()
            console.setFormatter(formatter)
            self.logger.addHandler(console)

    def send(self, message: str, *args):
        """Send a informational message to CATCH-APIs."""
        self.logger.info(message, *args)

    def error(self, message: str, *args):
        """Send an error message to CATCH-APIs."""
        self.logger.error(message, *args)

    def debug(self, message: str, *args):
        """Send a debugging message to CATCH-APIs."""
        self.logger.debug(message, *args)
