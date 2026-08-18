"""Module for configuring and providing a logger for the application."""

import logging
import sys

from src.config import settings


def setup_logger(name: str = "poly_orderbook",
                 level: int = logging.INFO) -> logging.Logger:
    """Configures and returns a logger instance.

    Args:
        name (str): The name of the logger. Defaults to "poly_orderbook".
        level (int): The logging level. Defaults to logging.INFO.

    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger(name)
    
    # If logger already has handlers, don't add more (prevents duplicate logs)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Create console handler and set level
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Add formatter to handler
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    return logger

# Create a default logger instance for the project
log_level = logging.DEBUG if settings.APP_ENV == "dev" else logging.INFO
logger = setup_logger(level=log_level)
