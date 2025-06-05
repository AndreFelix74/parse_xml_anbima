#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 09:34:13 2025

@author: andrefelix
"""


from contextlib import contextmanager
import time


@contextmanager
def log_step_context():
    start = time.time()
    yield lambda: round(time.time() - start, 2)
