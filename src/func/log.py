"""Initializes logging
"""

from logging import DEBUG, Logger, getLogger
from logging.config import dictConfig

import watchtower

from src.config import config

# global logger
LOGGER: Logger = getLogger(config.project_name)


def setup_logger(loglevel: int = DEBUG) -> watchtower.CloudWatchLogHandler:
    """
    Sets up logger configuration for log to CloudWatch
    """

    dictConfig(config.logging)
    LOGGER.setLevel(loglevel)

    cw_handler = watchtower.CloudWatchLogHandler()
    cw_handler.formatter.add_log_record_attrs = [
        "levelname",
        "filename",
        "funcName",
        "lineno",
    ]

    LOGGER.addHandler(cw_handler)
    return cw_handler


def close_handler(handler: watchtower.CloudWatchLogHandler) -> None:
    """
    Closes logger handler
    """

    handler.close()
    LOGGER.removeHandler(handler)
