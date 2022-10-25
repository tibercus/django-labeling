"""A decorator to help log time, that function was running."""
import logging
from typing import Callable, Any

from django.utils import timezone

import mylabelsystem.settings as settings


def _get_logger(level="info") -> Callable[[str], None]:
    """Function that checks if logger is configured and if it is, returns
    a logger. Otherwise returns Python's print.

    TODO - костыль. Надо нормально сконфигурировать логгер на сервере."""
    if hasattr(settings, "LOGGING"):
        logger = getattr(logging, level)
    else:
        logger = print

    return lambda message: logger(message)


def timeit(job_name: str) -> Callable:
    def decorator(function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            logger = _get_logger()

            start_time = timezone.now()
            logger(f"{start_time} - {job_name} started.")

            # TODO add exception handling for full logging.
            result = function(*args, **kwargs)

            finish_time = timezone.now()
            logger(f"{finish_time} - {job_name} ended.")
            logger(
                f"{finish_time} - "
                f"Took {(finish_time - start_time).total_seconds()} seconds."
            )
            return result

        return wrapper
    return decorator
