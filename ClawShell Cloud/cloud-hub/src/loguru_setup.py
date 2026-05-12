"""
Loguru logging setup for Cloud Hub.
Bridges loguru with existing standard logging infrastructure.
"""
import sys
import logging
from loguru import logger


def setup_logging(level: str = "INFO", verbose: bool = False):
    """
    Configure loguru as the primary logger and bridge with standard logging.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        verbose: If True, include more detailed output
    """
    # Remove default loguru handler
    logger.remove()
    
    # Console output with format
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    
    logger.add(
        sys.stderr,
        format=log_format,
        level=level,
        colorize=True,
    )
    
    # Also route through standard logging for libraries that use it
    class LoguruHandler(logging.Handler):
        def emit(self, record):
            level_map = {
                logging.DEBUG: logger.debug,
                logging.INFO: logger.info,
                logging.WARNING: logger.warning,
                logging.ERROR: logger.error,
                logging.CRITICAL: logger.critical,
            }
            level_fn = level_map.get(record.levelno, logger.info)
            level_fn(record.getMessage())
    
    handler = LoguruHandler()
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    return logger


def get_logger(name: str = None):
    """
    Get a loguru logger instance.
    
    Args:
        name: Logger name (will appear in log output)
    
    Returns:
        Loguru logger instance
    """
    if name:
        return logger.bind(name=name)
    return logger


# Auto-setup on import when not running tests
if __name__ != "__main__" and "pytest" not in sys.modules:
    setup_logging()
