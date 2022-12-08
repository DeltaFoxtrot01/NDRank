import logging
from typing_extensions import Final

DEBUG_TS_LOGGER_NAME: Final = "debug_ts_logger"

"""The debugger logger is a logger that was created to measure execution time between 
different sections of code.

This logger is configured to print the timestamp at the time the message is produced.
"""

def _configure_handler() -> None:
    debug_logger: logging.Logger = logging.getLogger(DEBUG_TS_LOGGER_NAME)

    formatter = logging.Formatter('debug_logger_TS_MARKER-%(levelname)s: %(asctime)s - %(message)s')

    output_handler: logging.StreamHandler = logging.StreamHandler()
    output_handler.setFormatter(formatter)

    debug_logger.addHandler(output_handler)

    debug_logger.setLevel(logging.INFO)
    debug_logger.propagate = False

_configure_handler()

def set_debug_logger_as_debug() -> None:
    """Set debugger to the debug level
    """
    logger: logging.Logger = logging.getLogger(DEBUG_TS_LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

def get_ts_debug_handler() -> logging.Logger:
    """Returns debug handler that allows to track timestamps

    Returns:
        logging.Logger: the required logger
    """
    return logging.getLogger(DEBUG_TS_LOGGER_NAME)