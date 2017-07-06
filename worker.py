#!/usr/bin/env python

from lib.runrep import *


def worker():
    """Worker logic"""
    ex = RunrepExecutor()
    result = ex.run_rsl("#paydowns")
    print len(result.data)


if __name__ == '__main__':
    worker()

