"""dashboard home"""
from dateutil.relativedelta import relativedelta
from datetime import datetime

import pandas as pd

from ...log import logger
from .. import queue, processors


def get_processors_data(refresh=False):
    df = processors.data.load_data(refresh=refresh)
    return df


def get_queue_data(refresh=False):
    df = queue.data.load_data(refresh=refresh)
    return df


def load_options(df):
    return list(df.PROJECT.unique())
