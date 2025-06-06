#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 09:44:56 2025

@author: andrefelix
"""


import time
import uuid
from contextlib import contextmanager
import logging
import structlog


RUN_ID = str(uuid.uuid4())


logging.basicConfig(
    filename="logs/pipeline.log",
    filemode="a",
    format="%(message)s",
    level=logging.INFO
)


structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True
)


logger = structlog.get_logger().bind(run_id=RUN_ID)


@contextmanager
def log_timing(phase: str, step: str):
    """
    Context manager for logging the duration and status of a pipeline phase step.

    This function:
        - Starts a timer when entering the context.
        - Logs the duration (in seconds) and a success message upon normal exit.
        - Logs an error message with the exception details if an error occurs.
        - Prints the duration to stdout for quick inspection during execution.

    Args:
        phase (str): The name of the pipeline phase (e.g., 'load', 'enrich').
        step (str): The specific step within the phase being timed.

    Yields:
        structlog.BoundLogger: A logger pre-bound with `run_id`, `phase`, and `step`.
    """
    log = logger.bind(phase=phase, step=step)
    print(f".... {phase} - {step}", end='\r')
    start = time.time()
    try:
        yield log
        duration = round(time.time() - start, 2)
        log.info("timing", duration=duration)
        print(f"{duration:.2f}")
    except Exception as excpt:
        log.error("timing", status="fail", error=str(excpt))
        raise
