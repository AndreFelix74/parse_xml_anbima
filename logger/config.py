#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 09:44:56 2025

@author: andrefelix
"""


import time
import uuid
from contextlib import contextmanager
import structlog
import logging


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
    log = logger.bind(phase=phase, step=step)
    start = time.time()
    try:
        yield log
        duration = round(time.time() - start, 2)
        log.info("timing", duration=duration)
        print(f"{duration:.2f} - {phase} - {step}")
    except Exception as excpt:
        log.error("timing", status="fail", error=str(excpt))
        raise
