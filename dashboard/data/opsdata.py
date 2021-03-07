import subprocess
from io import StringIO
from datetime import datetime, timedelta
import os
import logging
import math

import humanize
import pandas as pd
import numpy as np

from .opsparams import SQUEUE_USER, UPLOAD_DIR


# Data sources are:
# SLURM (on ACCRE at Vanderbilt)
# Local Filesystem (as configured for DAX upload queue)
#
# Note that this app does not access XNAT or REDCap or any external source
#
# Data is cached in a pickle file named ops.pkl. This data is written when the
# app first starts and then any time the user clicks Refresh Data. It is read
# any time the user changes the data filtering.


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


SQUEUE_CMD = 'squeue -u '+','.join(SQUEUE_USER)+' --format="%all"'

DFORMAT = '%Y-%m-%d %H:%M:%S'

# we concat diskq status and squeue status to make a single status
# squeue states: CG,F, PR, S, ST
# diskq statuses: JOB_RUNNING, JOB_FAILED, NEED_TO_RUN, COMPLETE,
# UPLOADING, READY_TO_COMPLETE, READY_TO_UPLOAD
STATUS_MAP = {
    'COMPLETENONE': 'COMPLETE',
    'JOB_FAILEDNONE': 'FAILED',
    'JOB_RUNNINGCD': 'RUNNING',
    'JOB_RUNNINGCG': 'RUNNING',
    'JOB_RUNNINGF': 'RUNNING',
    'JOB_RUNNINGR': 'RUNNING',
    'JOB_RUNNINGNONE': 'RUNNING',
    'JOB_RUNNINGPD': 'PENDING',
    'NONENONE': 'WAITING',
    'READY_TO_COMPLETENONE': 'COMPLETE',
    'READY_TO_UPLOADNONE': 'COMPLETE'}

JOB_TAB_COLS = [
    'LABEL', 'PROJECT', 'STATUS', 'PROCTYPE', 'USER',
    'JOBID', 'TIME', 'WALLTIME', 'LASTMOD']

SQUEUE_COLS = [
    'NAME', 'ST', 'STATE', 'PRIORITY', 'JOBID', 'MIN_MEMORY',
    'TIME', 'SUBMIT_TIME', 'START_TIME', 'TIME_LIMIT', 'TIME_LEFT', 'USER']


# Get fresh data from slurm and disk
def get_job_data():
    # TODO: run each load in separate threads

    # Load tasks in diskq
    logging.debug('loading diskq')
    diskq_df = load_diskq_queue()

    # load squeue
    logging.debug('loading squeue')
    squeue_df = load_slurm_queue()

    # TODO: load xnat if we want to identify lost jobs in a separate tab

    # merge squeue data into task queue
    logging.debug('merging data')

    if diskq_df.empty and squeue_df.empty:
        logging.debug('both empty')
        df = pd.DataFrame(columns=diskq_df.columns.union(squeue_df.columns))
    elif diskq_df.empty:
        logging.debug('diskq empty')
        df = squeue_df.reindex(squeue_df.columns.union(diskq_df.columns), axis=1)
    elif squeue_df.empty:
        logging.debug('squeue empty')
        df = diskq_df.reindex(diskq_df.columns.union(squeue_df.columns), axis=1)
    else:
        df = pd.merge(diskq_df, squeue_df, how='outer', on=['LABEL', 'USER'])

    if not df.empty:
        # assessor label is delimited by "-x-", first element is project,
        # fourth element is processing type
        df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
        df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

        # Do this to avoid blanks in the table
        df['JOBID'].fillna('not launched', inplace=True)

        # create a concatenated status that maps to full status
        df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
        df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

    # Determine how long ago status changed
    # how long has it been running, pending, waiting or complete?

    # Minimize columns
    logging.debug('finishing data')
    df = df.reindex(columns=JOB_TAB_COLS)

    return df.sort_values('LABEL')


# Loads the dax queue from disk
def load_diskq_queue(status=None):
    task_list = list()

    for d, u in zip(UPLOAD_DIR, SQUEUE_USER):
        diskq_dir = os.path.join(d, 'DISKQ')
        batch_dir = os.path.join(diskq_dir, 'BATCH')

        for t in os.listdir(batch_dir):
            assr = os.path.splitext(t)[0]
            #logging.debug('load task:{}'.format(assr))
            task = load_diskq_task(diskq_dir, assr)
            task['USER'] = u
            task_list.append(task)

    if len(task_list) > 0:
        df = pd.DataFrame(task_list)
    else:
        df = pd.DataFrame(columns=[
            'LABEL', 'procstatus', 'jobid', 'jobnode', 'jobstartdate',
            'memused', 'walltimeused', 'WALLTIME', 'LASTMOD', 'USER'])

    return df


# Load a single task/job information from disk
def load_diskq_task(diskq, assr):
    return {
        'LABEL': assr,
        'procstatus': get_diskq_attr(diskq, assr, 'procstatus'),
        'jobid': get_diskq_attr(diskq, assr, 'jobid'),
        'jobnode': get_diskq_attr(diskq, assr, 'jobnode'),
        'jobstartdate': get_diskq_attr(diskq, assr, 'jobstartdate'),
        'memused': get_diskq_attr(diskq, assr, 'memused'),
        'walltimeused': get_diskq_attr(diskq, assr, 'walltimeused'),
        'WALLTIME': get_diskq_walltime(diskq, assr),
        'LASTMOD': get_diskq_lastmod(diskq, assr)}


# Load slurm data
def load_slurm_queue():
    try:
        cmd = SQUEUE_CMD
        result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE)
        _data = result.stdout.decode('utf-8')
        df = pd.read_csv(
            StringIO(_data), delimiter='|', usecols=SQUEUE_COLS)
        df['LABEL'] = df['NAME'].str.split('.slurm').str[0]
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=SQUEUE_COLS+['LABEL'])


def get_diskq_walltime(diskq, assr):
    COOKIE = "#SBATCH --time="
    walltime = None
    bpath = os.path.join(diskq, 'BATCH', assr + '.slurm')

    try:
        with open(bpath, 'r') as f:
            for line in f:
                if line.startswith(COOKIE):
                    tmptime = line.split('=')[1].replace('"', '').replace("'", '')
                    walltime = humanize_walltime(tmptime)
                    break
    except IOError:
        logging.warn('file does not exist:' + bpath)
        return None
    except PermissionError:
        logging.warn('permission error reading file:' + bpath)
        return None

    return walltime


def humanize_walltime(walltime):
    tmptime = walltime
    days = 0
    hours = 0
    mins = 0

    if '-' in tmptime:
        tmpdays, tmptime = tmptime.split('-', 1)
        days = int(tmpdays)
    if ':' in walltime:
        tmphours, tmptime = tmptime.split(':', 1)
        hours = int(tmphours)
    if ':' in walltime:
        tmpmins = tmptime.split(':', 1)[0]
        mins = int(tmpmins)

    delta = timedelta(days=days, hours=hours, minutes=mins)
    return humanize.naturaldelta(delta)


def humanize_memused(memused):
    return humanize.naturalsize(memused)


def humanize_minutes(minutes):
    return humanize.naturaldelta(timedelta(minutes=minutes))


def get_diskq_lastmod(diskq, assr):

    if os.path.exists(os.path.join(diskq, 'procstatus', assr)):
        apath = os.path.join(diskq, 'procstatus', assr)
    elif os.path.exists(os.path.join(diskq, 'BATCH', assr + '.slurm')):
        apath = os.path.join(diskq, 'BATCH', assr + '.slurm')
    else:
        return None

    updatetime = datetime.fromtimestamp(os.path.getmtime(apath))
    delta = datetime.now() - updatetime
    return humanize.naturaldelta(delta)


def get_diskq_attr(diskq, assr, attr):
    apath = os.path.join(diskq, attr, assr)

    if not os.path.exists(apath):
        return None

    try:
        with open(apath, 'r') as f:
            return f.read().strip()
    except PermissionError:
        return None


def set_time(row):
    if pd.notna(row['SUBMIT_TIME']):
        startdt = datetime.strptime(
            str(row['SUBMIT_TIME']), '%Y-%m-%dT%H:%M:%S')
        row['submitdt'] = datetime.strftime(startdt, DFORMAT)

    row['timeused'] = row['TIME']

    return row


def clean_values(df):

    df['MEM'] = df['MEMUSED'].apply(clean_mem)

    # Cleanup wall time used to just be number of minutes
    df['TIMEUSED'] = df['WALLTIMEUSED'].apply(clean_timeused)

    df['TIME'] = df['TIMEUSED'].apply(clean_time)

    df['STARTDATE'] = df['JOBSTARTDATE'].apply(clean_startdate)

    df['TIMEDELTA'] = pd.to_timedelta(df['TIMEUSED'], 'm')

    df['ENDDATE'] = df['STARTDATE'] + df['TIMEDELTA']

    df['DATETIME'] = df['ENDDATE'].apply(clean_enddate)

    return df


def clean_enddate(enddate):
    return datetime.strftime(enddate, DFORMAT)


def clean_startdate(jobstartdate):
    return datetime.strptime(jobstartdate, '%Y-%m-%d')


def clean_mem(memused):
    try:
        bytes_used = int(float(memused)) * 1024
    except ValueError:
        bytes_used = np.nan

    return humanize_memused(bytes_used)


def clean_time(timeused):
    return humanize_minutes(int(timeused))


def clean_timeused(timeused):
    # Cleanup wall time used to just be number of minutes
    try:
        if '-' in timeused:
            t = datetime.strptime(timeused, '%j-%H:%M:%S')
            delta = timedelta(
                days=t.day,
                hours=t.hour, minutes=t.minute, seconds=t.second)
        else:
            t = datetime.strptime(timeused, '%H:%M:%S')
            delta = timedelta(
                hours=t.hour, minutes=t.minute, seconds=t.second)

        return math.ceil(delta.total_seconds() / 60)
    except ValueError:
        return 1


# TODO: move this to a shared function that takes app as argument
def get_filename():
    return '{}.pkl'.format('ops')


# TODO: move this to a shared function that takes app as argument
def save_data(df):
    filename = get_filename()

    # save to cache
    df.to_pickle(filename)
    return df


# TODO: move this to a shared function that takes filename argument
def load_data():
    filename = get_filename()

    if os.path.exists(filename):
        df = pd.read_pickle(filename)
    else:
        df = refresh_data()

    return df


def refresh_data():
    df = get_job_data()

    # save to cache
    save_data(df)

    return df
