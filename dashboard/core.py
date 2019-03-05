# coding=utf-8
import os
import math
from datetime import datetime, timedelta
from glob import glob
import tempfile
from zipfile import BadZipfile
import fileinput
import subprocess

import yaml
import json
import urllib
import pandas as pd
import plotly
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
from dax import XnatUtils


def write_report(proj_list, assr_types, scan_types, datafile):
    ASSR_URI = '/REST/experiments?project={}&xsiType=proc:genprocdata&\
columns=ID,label,URI,xsiType,project,\
xnat:imagesessiondata/subject_id,\
xnat:imagesessiondata/id,xnat:imagesessiondata/label,\
proc:genprocdata/procstatus,proc:genprocdata/proctype,\
proc:genprocdata/validation/status,proc:genprocdata/procversion,\
proc:genprocdata/jobstartdate,proc:genprocdata/memused,\
proc:genprocdata/walltimeused,proc:genprocdata/jobid,proc:genprocdata/inputs,\
proc:genprocdata/meta/last_modified'
    ASSR_RENAME = {
        'ID': 'ID',
        'URI': 'URI',
        'label': 'label',
        'proc:genprocdata/inputs': 'inputs',
        'proc:genprocdata/jobid': 'jobid',
        'proc:genprocdata/jobstartdate': 'jobstartdate',
        'proc:genprocdata/memused': 'memused',
        'proc:genprocdata/procstatus': 'procstatus',
        'proc:genprocdata/proctype': 'proctype',
        'proc:genprocdata/procversion': 'procversion',
        'proc:genprocdata/validation/status': 'qcstatus',
        'proc:genprocdata/walltimeused': 'walltimeused',
        'project': 'project',
        'session_label': 'session',
        'xnat:imagesessiondata/subject_id': 'subject',
        'proc:genprocdata/meta/last_modified': 'last_modified'
    }
    SCAN_URI = '/data/archive/experiments?project={}&\
xsiType=xnat:imageSessionData&\
columns=ID,URI,label,subject_label,project,\
xnat:imagescandata/id,xnat:imagescandata/type,xnat:imagescandata/quality,\
xnat:imagescandata/series_description,xnat:imageScanData/meta/last_modified'
    SCAN_RENAME = {
        'ID': 'ID',
        'URI': 'URI',
        'label': 'session',
        'subject_label': 'subject',
        'project': 'project',
        'xnat:imagescandata/id': 'scan_id',
        'xnat:imagescandata/type': 'type',
        'xnat:imagescandata/quality': 'quality',
        'xnat:imagescandata/series_description': 'scan_description',
        'xnat:imageScanData/meta/last_modified': 'last_modified'
    }

    name = os.path.splitext(os.path.basename(datafile))[0]
    data = {}
    data['projects'] = {}
    data['updatetime'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

    print('INFO:{}:connecting to XNAT'.format(name))
    xnat = XnatUtils.get_interface()

    for proj in proj_list:
        print('INFO:{}:handling project:{}'.format(name, proj))

        # Create project in new data
        data['projects'][proj] = {}

        # Extract scan data
        print('INFO:{}:extracting scan data:{}'.format(name, proj))
        _uri = SCAN_URI.format(proj)
        _df = pd.DataFrame(xnat._get_json(_uri))

        # Rename columns
        _df.rename(columns=SCAN_RENAME, inplace=True)

        if not _df.empty and scan_types:
            # Filter scan types to include
            _df = _df[_df['type'].isin(scan_types)]

        # Append to project data
        data['projects'][proj]['scan'] = _df.to_dict('records')

        # Extract assr data
        print('INFO:{}:extracting assr data:{}'.format(name, proj))
        _uri = ASSR_URI.format(proj)
        _df = pd.DataFrame(xnat._get_json(_uri))

        # Rename columns
        _df.rename(columns=ASSR_RENAME, inplace=True)
        if not _df.empty and assr_types:
            # Filter scan types to include
            _df = _df[_df['proctype'].isin(assr_types)]

        # Append to project data
        data['projects'][proj]['assr'] = _df.to_dict('records')
        assr_list = data['projects'][proj]['assr']

        # LST
        if 'LST_v1' in assr_types:
            print('INFO:{}:extracting LST data:{}'.format(name, proj))
            _list = [a for a in assr_list if a['proctype'] == 'LST_v1']
            _stats = init_stats(_list, xnat)
            data['projects'][proj]['lst'] = _stats

        # fMRI
        if 'fMRIQA_v3' in assr_types:
            print('INFO:{}:extracting fMRI data:{}'.format(name, proj))
            _list = [a for a in assr_list if a['proctype'] == 'fMRIQA_v3']
            _stats = init_stats(_list, xnat)
            data['projects'][proj]['fmri'] = _stats

        # EDAT
        if 'EDATQA_v1' in assr_types:
            print('INFO:{}:extracting EDAT data:{}'.format(name, proj))
            _list = [a for a in assr_list if a['proctype'] == 'EDATQA_v1']
            _stats = init_stats(_list, xnat)
            data['projects'][proj]['edat'] = _stats

    # Write updated data file
    print('INFO:{}:finished extracting, saving'.format(name))
    with open(datafile, 'w') as outfile:
        json.dump(data, outfile)

    print('INFO:{}:DONE!'.format(name))


def init_stats(assr_list, xnat):
    _stats = []

    # Check each lst
    for assr in assr_list:
        # Load from xnat
        print('INFO:loading stats:' + assr['label'])
        _stats.append(load_stat(assr, xnat))

    return _stats


def load_keyvalue_data(data_file):
    data = dict()

    for line in fileinput.input(data_file):
        line = line.strip()
        if ',' in line:
            stringline = line.split(',')
        else:
            stringline = line.split('=')

        if len(stringline) == 2:
            # add the value in the dictionary
            data[stringline[0]] = stringline[1]
        elif len(stringline) == 3:
            # add the value in the dictionary
            data[stringline[0]] = stringline[2]

    return data


def load_stats_data(res):
    _stats = {}
    _dir = tempfile.mkdtemp()

    # Download the files and load the data
    try:
        file_list = res.get(_dir, extract=True)
        for f in file_list:
            _stats.update(load_keyvalue_data(f))
    except BadZipfile:
        print('DEBUG:bad zip file')

    return _stats


def load_stat(assr, xnat):
    RES_URI = '/projects/{}/subjects/{}/experiments/{}/assessors/{}/\
out/resources/{}'
    _uri = RES_URI.format(
        assr['project'],
        assr['subject'],
        assr['session'],
        assr['label'],
        'STATS')
    res = xnat.select(_uri)

    if not res.exists():
        assr['NOTES'] = 'No stats resource'
    else:
        _stats = load_stats_data(res)
        assr.update(_stats)

    # Try to match each assr with a scan and append the scantype
    labels = assr['label'].split('-x-')
    if len(labels) == 5:
        scan_obj = xnat.select(
            '/projects/' + labels[0] +
            '/subjects/' + labels[1] +
            '/experiments/' + labels[2] +
            '/scans/' + labels[3]
        )
        if scan_obj.exists():
            assr['scan_type'] = scan_obj.attrs.get('type')
            assr['scan_descrip'] = scan_obj.attrs.get('series_description')
    elif len(labels) == 4 and assr['inputs']:
        # Try to interpret scan from inputs field
        assr_inputs = json.loads(assr['inputs'].replace('&quot;', '"'))
        _key, _val = assr_inputs.popitem()
        scan_id = _val.split('/')[-1]
        labels = assr['label'].split('-x-')
        scan_obj = xnat.select(
            '/projects/' + labels[0] +
            '/subjects/' + labels[1] +
            '/experiments/' + labels[2] +
            '/scans/' + scan_id
        )
        if scan_obj.exists():
            assr['scan_type'] = scan_obj.attrs.get('type')
            assr['scan_descrip'] = scan_obj.attrs.get('series_description')

    return assr


class DashboardData:
    MOD_URI = '/data/archive/experiments?project={}&columns=last_modified'
    TASK_IGNORE_LIST = ['NO_DATA', 'NEED_INPUTS', 'Complete']
    DFORMAT = '%Y-%m-%d %H:%M:%S'
    TASK_COLS = [
        'label', 'project', 'memused(MB)',
        'procstatus', 'proctype', 'datetime', 'timeused(min)']
    ASSR_URI = '/REST/experiments?project={}&xsiType=proc:genprocdata&\
columns=ID,label,URI,xsiType,project,\
xnat:imagesessiondata/subject_id,\
xnat:imagesessiondata/id,xnat:imagesessiondata/label,\
proc:genprocdata/procstatus,proc:genprocdata/proctype,\
proc:genprocdata/validation/status,proc:genprocdata/procversion,\
proc:genprocdata/jobstartdate,proc:genprocdata/memused,\
proc:genprocdata/walltimeused,proc:genprocdata/jobid,proc:genprocdata/inputs,\
proc:genprocdata/meta/last_modified'
    ASSR_RENAME = {
        'ID': 'ID',
        'URI': 'URI',
        'label': 'label',
        'proc:genprocdata/inputs': 'inputs',
        'proc:genprocdata/jobid': 'jobid',
        'proc:genprocdata/jobstartdate': 'jobstartdate',
        'proc:genprocdata/memused': 'memused',
        'proc:genprocdata/procstatus': 'procstatus',
        'proc:genprocdata/proctype': 'proctype',
        'proc:genprocdata/procversion': 'procversion',
        'proc:genprocdata/validation/status': 'qcstatus',
        'proc:genprocdata/walltimeused': 'walltimeused',
        'project': 'project',
        'session_label': 'session',
        'xnat:imagesessiondata/subject_id': 'subject',
        'proc:genprocdata/meta/last_modified': 'last_modified'
    }
    SCAN_URI = '/data/archive/experiments?project={}&\
xsiType=xnat:imageSessionData&\
columns=ID,URI,label,subject_label,project\
xnat:imagescandata/id,xnat:imagescandata/type,xnat:imagescandata/quality,\
xnat:imagescandata/series_description,xnat:imageScanData/meta/last_modified'
    SCAN_RENAME = {
        'ID': 'ID',
        'URI': 'URI',
        'label': 'session',
        'subject_label': 'subject',
        'project': 'project',
        'xnat:imagescandata/id': 'scan_id',
        'xnat:imagescandata/type': 'type',
        'xnat:imagescandata/quality': 'quality',
        'xnat:imagescandata/series_description': 'scan_description',
        'xnat:imageScanData/meta/last_modified': 'last_modified'
    }
    RES_URI = '/projects/{}/subjects/{}/experiments/{}/assessors/{}/\
out/resources/{}'
    SCANTYPE_URI = '/data/archive/experiments?project={}&\
xsiType=xnat:imageSessionData&\
columns=ID,label,project,xnat:imagescandata/id,xnat:imagescandata/type'
    ASSRTYPE_URI = '/data/archive/experiments?project={}&\
xsiType=proc:genprocdata&columns=ID,xsiType,project,proc:genprocdata/proctype'

    def __init__(self, config, xnat):
        self.xnat = xnat
        self.config = config

        # Determine project list
        if 'xnat_projects' not in self.config:
            proj_list = self.get_projects()
        elif self.config['xnat_projects'] == 'favorites':
            # Load projects that have been checked as Favorite for current user
            print('INFO:Loading project list from XNAT')
            proj_list = [x['id'] for x in self.xnat._get_json(self.FAV_URI)]
        elif isinstance(self.config['xnat_projects'], basestring):
            proj_list = [self.config['xnat_projects']]
        else:
            proj_list = self.config['xnat_projects']

        self.all_atype_list = self.get_assr_types(proj_list)
        self.all_stype_list = self.get_scan_types(proj_list)
        self.all_proj_list = proj_list
        self.datadir = self.config['data_dir']
        self.updatetime = ''
        self.datafile = None
        self.assr_df = None
        self.task_df = None
        self.scan_df = None
        if 'use_squeue' in self.config and self.config['use_squeue']:
            self.use_squeue = True
        else:
            self.use_squeue = False

    def get_projects(self):
        ROLE_LIST = ['Owners', 'Members', 'Collaborators']
        _uri = '/data/projects?accessible=True'
        _df = pd.DataFrame(self.xnat._get_json(_uri))
        _df = _df[_df['user_role_6'].isin(ROLE_LIST)]
        return list(_df.id)

    def get_scan_types(self, proj_list):
        _uri = self.SCANTYPE_URI.format(','.join(proj_list))
        _df = pd.DataFrame(self.xnat._get_json(_uri))
        _df.rename(columns=self.SCAN_RENAME, inplace=True)
        return sorted(list(set(_df.type)))

    def get_assr_types(self, proj_list):
        _uri = self.ASSRTYPE_URI.format(','.join(proj_list))
        _df = pd.DataFrame(self.xnat._get_json(_uri))
        _df.rename(columns=self.ASSR_RENAME, inplace=True)
        return sorted(list(set(_df.proctype)))

    def save_data(self, data, datafile):
        with open(datafile, 'w') as outfile:
            json.dump(data, outfile)

    def init_data(self):
        nowtime = datetime.now()
        newdata = {}
        newdata['projects'] = {}
        newdata['updatetime'] = self.formatted_time(nowtime)
        newdatafile = self.data_filename(nowtime)

        print('DEBUG:init_data():begin=' + self.now_formatted())

        for proj in self.all_proj_list:
            print('INFO:init project data:' + proj)

            # Create project in new data
            newdata['projects'][proj] = {}

            # Extract scan data
            print('INFO:extracting scan data:' + proj)
            newdata['projects'][proj]['scan'] = self.extract_project_scan(proj)

            # Extract assr data
            print('INFO:extracting assr data:' + proj)
            assr_list = self.extract_project_assr(proj)
            newdata['projects'][proj]['assr'] = assr_list

            # LST
            print('INFO:extracting LST data:' + proj)
            _list = [a for a in assr_list if a['proctype'] == 'LST_v1']
            _stats = self.init_stats(_list)
            newdata['projects'][proj]['lst'] = _stats

            # fMRI
            print('INFO:extracting fMRI data:' + proj)
            _list = [a for a in assr_list if a['proctype'] == 'fMRIQA_v3']
            _stats = self.init_stats(_list)
            newdata['projects'][proj]['fmri'] = _stats

            # EDAT
            print('INFO:extracting EDAT data:' + proj)
            _list = [a for a in assr_list if a['proctype'] == 'EDATQA_v1']
            _stats = self.init_stats(_list)
            newdata['projects'][proj]['edat'] = _stats

        print('INFO:finished init data, now saving')
        print('DEBUG:init_data():end=' + self.now_formatted())

        # Write updated data file
        self.save_data(newdata, newdatafile)

        return newdatafile

    def load_data(self, datafile):
        assr_list = list()
        scan_list = list()
        fmri_list = list()
        lst_list = list()
        edat_list = list()
        self.datafile = datafile

        print('INFO:data file=' + datafile)
        with open(datafile) as f:
            data = json.load(f)

        self.updatetime = data['updatetime']

        # Build lists from projects
        for proj in data['projects'].keys():
            if proj not in self.all_proj_list:
                print('INFO:ignoring project data, not in config')
                continue

            assr_list.extend(data['projects'][proj]['assr'])
            scan_list.extend(data['projects'][proj]['scan'])
            if 'fmri' in data['projects'][proj]:
                fmri_list.extend(data['projects'][proj]['fmri'])
            if 'lst' in data['projects'][proj]:
                lst_list.extend(data['projects'][proj]['lst'])
            if 'edat' in data['projects'][proj]:
                edat_list.extend(data['projects'][proj]['edat'])

        if self.use_squeue:
            self.squeue_df = pd.DataFrame(data['squeue'])
        else:
            self.squeue_df = None

        # Load assessors
        _cols = [
            'proctype', 'label', 'qcstatus', 'project', 'session',
            'procstatus', 'jobstartdate', 'jobid', 'memused', 'version',
            'walltimeused']
        self.assr_df = pd.DataFrame(assr_list, columns=_cols)
        self.assr_df['qcstatus'].replace({
            'Passed': 'P', 'passed': 'P',
            'Questionable': 'P',
            'Failed': 'F',
            'Needs QA': 'Q'},
            inplace=True)
        self.assr_df.loc[
            (self.assr_df['qcstatus'].str.len() > 1), 'qcstatus'] = 'J'
        self.assr_df['name'] = self.assr_df['label'] + '.slurm'
        self.assr_dfp = self.assr_df.pivot_table(
            index=('session', 'project'),
            columns='proctype', values='qcstatus',
            aggfunc=lambda q: ''.join(q))
        self.assr_dfp.reset_index(inplace=True)

        # Load fmri
        _cols = [
            'label',
            'project',
            'session',
            'qcstatus',
            'scan_type',
            'fmriqa_v3_voxel_displacement_median',
            'fmriqa_v3_voxel_displacement_95prctile',
            'fmriqa_v3_voxel_displacement_99prctile',
            'fmriqa_v3_signal_delta_95prctile',
            'fmriqa_v3_global_timeseries_stddev',
            'fmriqa_v3_tsnr_95prctile',
            'fmriqa_v3_tsnr_median']
        self.fmri_df = pd.DataFrame(fmri_list, columns=_cols)
        self.fmri_df.rename(
            columns={
                'fmriqa_v3_voxel_displacement_median': 'displace_median',
                'fmriqa_v3_voxel_displacement_95prctile': 'displace_95',
                'fmriqa_v3_voxel_displacement_99prctile': 'displace_99',
                'fmriqa_v3_signal_delta_95prctile': 'sig_delta_95',
                'fmriqa_v3_global_timeseries_stddev': 'glob_ts_stddev',
                'fmriqa_v3_tsnr_95prctile': 'tsnr_95',
                'fmriqa_v3_tsnr_median': 'tsnr_median'
            }, inplace=True)

        # Load LST
        _cols = [
            'label', 'project', 'session', 'qcstatus', 'wml_volume'
        ]
        self.lst_df = pd.DataFrame(lst_list, columns=_cols)

        # Load EDAT
        _cols = [
            'label',
            'project',
            'session',
            'qcstatus',
            'scan_type',
            'edatqa_acc_mean',
            'edatqa_rt_mean',
            'edatqa_trial_count'

        ]
        self.edat_df = pd.DataFrame(edat_list, columns=_cols)
        self.edat_df.rename(
            columns={
                'edatqa_acc_mean': 'acc_mean',
                'edatqa_rt_mean': 'rt_mean',
                'edatqa_trial_count': 'trial_count'
            }, inplace=True)

        # Load scan data
        _cols = ['session', 'project', 'quality', 'type']
        self.scan_df = pd.DataFrame(scan_list, columns=_cols)
        self.scan_df['quality'].replace(
            {'usable': 'P', 'unusable': 'F', 'questionable': 'Q'},
            inplace=True)
        self.scan_dfp = self.scan_df.pivot_table(
            index=('session', 'project'),
            columns='type', values='quality',
            aggfunc=lambda x: ''.join(x))
        self.scan_dfp.reset_index(inplace=True)

        # Merge assr and squeue
        if self.use_squeue:
            self.task_df = pd.merge(
                self.assr_df, self.squeue_df, how='outer', on='name')
        else:
            self.task_df = self.assr_df

        # Filter out tasks we want to ignore
        self.task_df = self.task_df[~self.task_df.procstatus.isin(
            self.TASK_IGNORE_LIST)]

        if self.task_df.empty:
            self.task_df = self.task_df.reindex(columns=self.TASK_COLS)
        else:
            # Apply the clean values
            self.task_df = self.task_df.apply(self.clean_values, axis=1)
            # Minimize columns
            self.task_df = self.task_df[self.TASK_COLS]

        self.test_df = self.assr_df.copy()
        self.test_dfp = self.assr_dfp.copy()

    def selected_projects(self):
        if self.assr_df is None:
            return self.all_proj_list
        else:
            return sorted(set(
                list(self.assr_df.project.unique()) +
                list(self.scan_df.project.unique())))

    def selected_assr_types(self):
        if self.assr_df is None:
            return self.all_atype_list
        else:
            return sorted(list(self.assr_df.proctype.unique()))

    def selected_scan_types(self):
        if self.scan_df is None:
            return self.all_stype_list
        else:
            return sorted(list(self.scan_df.type.unique()))

    def updated_datetime(self):
        return datetime.strptime(self.updatetime, self.DFORMAT)

    def now_formatted(self):
        return datetime.strftime(datetime.now(), self.DFORMAT)

    def data_filename(self, curtime):
        ftime = datetime.strftime(curtime, '%Y%m%d-%H%M%S')
        return os.path.join(self.datadir, 'data-' + ftime + '.json')

    def formatted_time(self, curtime):
        return datetime.strftime(curtime, self.DFORMAT)

    def latest_datafile(self):
        return sorted(glob(self.datadir + '/data*.json'), reverse=True)[0]

    def update_data(self, fullupdate=False):
        print('DEBUG:update_data():start=' + self.now_formatted())
        nowtime = datetime.now()
        newdata = {}
        newdata['projects'] = {}
        newdata['updatetime'] = self.formatted_time(nowtime)
        datafile = None
        dirty = False

        try:
            # Load latest data from file
            datafile = self.latest_datafile()
            print('INFO:datafile=' + datafile)
            with open(datafile) as f:
                olddata = json.load(f)

            prevtime = olddata['updatetime']
        except IndexError:
            print('INFO:existing data file not found.')
            return

        for proj in self.all_proj_list:
            # Skip if not modified
            lastmod = self.proj_last_modified(proj)
            if lastmod < prevtime and proj in olddata['projects'] and False:
                print('INFO:skipping project, not modified:{}:{}'.format(
                    proj, lastmod))
                # Copy old data to new data
                newdata['projects'][proj] = olddata['projects'][proj]
                continue

            dirty = True

            print('INFO:re-loading project data:' + proj)

            # Create project in new data
            newdata['projects'][proj] = {}

            # Extract scan data
            print('INFO:extracting scan data:' + proj)
            newdata['projects'][proj]['scan'] = self.extract_project_scan(proj)

            # Extract assr data
            print('INFO:extracting assr data:' + proj)
            assr_list = self.extract_project_assr(proj)
            newdata['projects'][proj]['assr'] = assr_list

            # LST
            print('INFO:extracting LST data:' + proj)
            try:
                old_stats = olddata['projects'][proj]['lst']
            except KeyError:
                old_stats = list()

            _list = [a for a in assr_list if a['proctype'] == 'LST_v1']
            _stats = self.update_stats(old_stats, _list, prevtime)
            newdata['projects'][proj]['lst'] = _stats

            # fMRI
            print('INFO:extracting fMRI data:' + proj)
            try:
                old_stats = olddata['projects'][proj]['fmri']
            except KeyError:
                old_stats = list()

            _list = [a for a in assr_list if a['proctype'] == 'fMRIQA_v3']
            _stats = self.update_stats(old_stats, _list, prevtime)
            newdata['projects'][proj]['fmri'] = _stats

            # EDAT
            print('INFO:extracting EDAT data:' + proj)
            try:
                old_stats = olddata['projects'][proj]['edat']
            except KeyError:
                old_stats = list()

            _list = [a for a in assr_list if a['proctype'] == 'EDATQA_v1']
            _stats = self.update_stats(old_stats, _list, prevtime)
            newdata['projects'][proj]['edat'] = _stats

        print('DEBUG:update_data():complete=' + self.now_formatted())

        if dirty:
            print('INFO:finished extracting new data, now saving')

            # Write updated data file
            self.save_data(newdata, datafile)

            # Reload data
            self.load_data()
            print('DEBUG:update_data():end=' + self.now_formatted())
        else:
            print('INFO:nothing to update')

    def extract_project_assr(self, proj):
        # Extract assr data
        _uri = self.ASSR_URI.format(proj)
        _df = pd.DataFrame(self.xnat._get_json(_uri))
        _df.rename(columns=self.ASSR_RENAME, inplace=True)
        return _df.to_dict('records')

    def extract_project_scan(self, proj):
        # Extract scan data
        _uri = self.SCAN_URI.format(proj)
        _df = pd.DataFrame(self.xnat._get_json(_uri))
        _df.rename(columns=self.SCAN_RENAME, inplace=True)
        return _df.to_dict('records')

    def init_stats(self, assr_list):
        _stats = []

        # Check each lst
        for assr in assr_list:
            # Load from xnat
            print('DEBUG:loading stats from XNAT:' + assr['label'])
            _stats.append(self.load_stat(assr))

        return _stats

    def update_stats(self, old_stats, assr_list, prevtime):
        new_stats = []

        # Check each assr
        for assr in assr_list:
            news = None
            if assr['last_modified'] < prevtime:
                # Find it in old stats
                for olds in old_stats:
                    if olds['label'] == assr['label']:
                        # Copy old to new
                        news = olds
                        break

            if not news:
                # Load from xnat
                print('DEBUG:loading stat from XNAT:' + assr['label'])
                news = self.load_stat(assr)

            # Save to list
            new_stats.append(news)

        return new_stats

    def proj_last_modified(self, project):
        _uri = self.MOD_URI.format(project)

        # Get latest modified date from project sessions
        _df = pd.DataFrame(self.xnat._get_json(_uri))

        return _df['last_modified'].max()

    def load_keyvalue_data(self, data_file):
        data = dict()

        for line in fileinput.input(data_file):
            line = line.strip()
            if ',' in line:
                stringline = line.split(',')
            else:
                stringline = line.split('=')

            if len(stringline) == 2:
                # add the value in the dictionary
                data[stringline[0]] = stringline[1]
            elif len(stringline) == 3:
                # add the value in the dictionary
                data[stringline[0]] = stringline[2]

        return data

    def load_stats_data(self, res):
        _stats = {}
        _dir = tempfile.mkdtemp()

        # Download the files and load the data
        try:
            file_list = res.get(_dir, extract=True)
            for f in file_list:
                _stats.update(self.load_keyvalue_data(f))
        except BadZipfile:
            print('DEBUG:bad zip file')

        return _stats

    def load_stat(self, assr):
        _uri = self.RES_URI.format(
            assr['project'],
            assr['subject'],
            assr['session'],
            assr['label'],
            'STATS')
        res = self.xnat.select(_uri)

        if not res.exists():
            assr['NOTES'] = 'No stats resource'
        else:
            _stats = self.load_stats_data(res)
            assr.update(_stats)

        # Try to match each assr with a scan and append the scantype
        labels = assr['label'].split('-x-')
        scan_obj = self.xnat.select(
            '/projects/' + labels[0] +
            '/subjects/' + labels[1] +
            '/experiments/' + labels[2] +
            '/scans/' + labels[3]
        )

        if not scan_obj.exists() and assr['inputs']:
            # Try to interpret scan from inputs field
            assr_inputs = json.loads(assr['inputs'].replace('&quot;', '"'))
            _key, _val = assr_inputs.popitem()
            scan_id = _val.split('/')[-1]
            labels = assr['label'].split('-x-')
            scan_obj = self.xnat.select(
                '/projects/' + labels[0] +
                '/subjects/' + labels[1] +
                '/experiments/' + labels[2] +
                '/scans/' + scan_id
            )

        if scan_obj.exists():
            assr['scan_type'] = scan_obj.attrs.get('type')
            assr['scan_descrip'] = scan_obj.attrs.get('series_description')

        return assr

    def clean_values(self, row):
        # Clean up memory-used to be just number of megabytes
        try:
            if str(row['memused']).endswith('mb'):
                row['memused(MB)'] = row['memused'][0:-2]
            else:
                row['memused(MB)'] = math.ceil(float(row['memused']) / 1024)
        except ValueError:
            row['memused(MB)'] = 1

        # Cleanup wall time used to just be number of minutes
        try:
            if '-' in str(row['walltimeused']):
                t = datetime.strptime(str(row['walltimeused']), '%j-%H:%M:%S')
                delta = timedelta(
                    days=t.day,
                    hours=t.hour, minutes=t.minute, seconds=t.second)
            else:
                t = datetime.strptime(str(row['walltimeused']), '%H:%M:%S')
                delta = timedelta(
                    hours=t.hour, minutes=t.minute, seconds=t.second)

            startdate = datetime.strptime(str(row['jobstartdate']), '%Y-%m-%d')
            enddate = startdate + delta
            row['datetime'] = datetime.strftime(enddate, self.DFORMAT)
            row['timeused(min)'] = math.ceil(delta.total_seconds() / 60)
        except ValueError:
            row['timeused(min)'] = 1
            if row['jobstartdate']:
                row['datetime'] = row['jobstartdate']
            else:
                row['datetime'] = self.updatetime

        return row


class DaxDashboard:
    GEN_TEMPLATE = '''
import sys
sys.path.insert(0, '{}')
from dashboard import write_report

projects = {}

atypes = {}

stypes = {}

datafile = '{}'

write_report(projects, atypes, stypes, datafile)
'''
    DFORMAT = '%Y-%m-%d %H:%M:%S'
    TASK_COLS = [
        'label', 'project', 'memused(MB)',
        'procstatus', 'proctype', 'datetime', 'timeused(min)']
    FAV_URI = '/data/archive/projects?favorite=True'

    def __init__(self, config_file, url_base_pathname=None):
        print('DEBUG:connecting to XNAT')
        self.xnat = XnatUtils.get_interface()
        self.config = None

        with open(config_file, 'r') as f:
            self.config = yaml.load(f)

        if not os.path.exists(self.config['data_dir']):
            raise IOError('Dir does not exist:' + self.config['data_dir'])

        self.dashdata = DashboardData(self.config, self.xnat)
        self.datadir = self.config['data_dir']
        self.app = None
        self.url_base_pathname = url_base_pathname
        self.build_app()

    def all_projects(self):
        return self.dashdata.all_proj_list

    def all_scan_types(self):
        return self.dashdata.all_stype_list

    def all_assr_types(self):
        return self.dashdata.all_atype_list

    def update_data(self):
        self.dashdata.update_data()

    def load_report_list(self):
        _list = glob(self.datadir + '/*.json')
        _list.sort(key=os.path.getmtime, reverse=True)
        return _list

    def make_options(self, values):
        return [{'label': x, 'value': x} for x in sorted(values)]

    def build_app(self):
        report_list = self.load_report_list()
        report_options = [{
            'label': os.path.splitext(os.path.basename(x))[0],
            'value': x} for x in report_list]
        if report_list:
            cur_report = report_list[0]
        else:
            cur_report = None

        # Make the main dash app
        app = dash.Dash(__name__)

        # Dash CSS
        app.css.append_css(
            {"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

        # Note: other css is loaded from assets folder

        app.title = 'DAX Dashboard'
        if self.url_base_pathname:
            app.config.update({
                'url_base_pathname': self.url_base_pathname,
                'routes_pathname_prefix': self.url_base_pathname,
                'requests_pathname_prefix': self.url_base_pathname
            })
        self.app = app

        # If you are assigning callbacks to components
        # that are generated by other callbacks
        # (and therefore not in the initial layout), then
        # you can suppress this exception by setting
        app.config['suppress_callback_exceptions'] = True

        app.layout = html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content')
        ])

        report_layout = html.Div([
            html.Div([
                html.H1(
                    'DAX Dashboard',
                    style={
                        'margin-right': '100px', 'display': 'inline-block'}),
                html.P(children=[
                    'Choose Report: ',
                    dcc.Dropdown(
                        id='dropdown-report-list',
                        options=report_options,
                        value=cur_report, clearable=False,
                        style={'display': 'inline-block', 'width': '400px'}),
                    dcc.Interval(
                        id='interval-component',
                        interval=10000,
                        n_intervals=0
                    ),
                    dcc.Link(
                        html.Button('Generate New Report'), href='/generate',
                        style={'margin-left': '25px', 'margin-right': '25px'})
                ], style={'float': 'right', 'display': 'inline-block'}),
            ], style={'display': 'inline-block'}),
            html.Div(children=['Just a moment...'], id='report-content')
        ])

        @app.callback(
            Output('dropdown-report-list', 'options'),
            [Input('interval-component', 'n_intervals')])
        def update_report_list(n):
            return [{
                'label': os.path.splitext(os.path.basename(x))[0],
                'value': x} for x in self.load_report_list()]

        @app.callback(
            Output('page-content', 'children'),
            [Input('url', 'pathname')])
        def display_page(pathname):
            if pathname == '/generate':
                return get_generate_layout()
            else:
                return report_layout

        def get_generate_layout():
            print('DEBUG:get_generate_layout')

            # Check for currently generating report,
            # python file exists but json file does not
            cur_script = self.script_running()
            if cur_script:
                return self.script_running_content()

            _list = self.all_projects()
            all_proj_options = [{'label': x, 'value': x} for x in _list]

            _list = self.all_scan_types()
            all_scan_options = [{'label': x, 'value': x} for x in _list]

            _list = self.all_assr_types()
            all_assr_options = [{'label': x, 'value': x} for x in _list]

            # Build it
            return html.Div([
                html.Div(id='generate-content'),
                html.Br(), html.Br(),
                html.Div([
                    # generate report header
                    html.Div([
                        html.Span(
                            html.H1("Generate New Report"),
                            style={"fontWeight": "bold", "fontSize": "20"})],
                        className="row",
                        style={"borderBottom": "1px solid"},
                    ),
                    # generate report form
                    html.Div(children=[
                        html.Br(),
                        html.P(children=["XNAT Host: ", self.xnat.host]),
                        html.P(children=["XNAT User: ", self.xnat.user]),
                        html.Br(),
                        html.P(children=[
                            html.H3("Projects: "),
                            dcc.Dropdown(
                                id='dropdown-generate-proj',
                                multi=True,
                                options=all_proj_options,
                                placeholder='Select projects: ',
                                value=self.dashdata.selected_projects()),
                            html.Br(), html.Br()]),
                        html.P(children=[
                            html.H3("Scan Types: "),
                            dcc.Dropdown(
                                id='dropdown-generate-scan-types',
                                multi=True,
                                options=all_scan_options,
                                placeholder='Select scan type(s)',
                                value=self.dashdata.selected_scan_types()),
                            html.Br(), html.Br()]),
                        html.P(children=[
                            html.H3("Assessor Types: "),
                            dcc.Dropdown(
                                id='dropdown-generate-assr-types', multi=True,
                                options=all_assr_options,
                                placeholder='Select assr type(s)',
                                value=self.dashdata.selected_assr_types()),
                            html.Br(), html.Br()]),
                        html.P(children=[
                            html.H3("Report Prefix: "),
                            dcc.Input(
                                id='dropdown-generate-prefix',
                                placeholder='Enter prefix for the new report',
                                value='',
                                style={'font-size': 'large', 'width': '33%'}),
                            html.Br(), html.Br()]),
                        html.Div(id='output-provider'),
                        html.Button(
                            children=['Submit'], type='submit',
                            id='button-generate-submit',
                            n_clicks=0, style={'margin-right': '25px'}),
                        dcc.Link(html.Button('Cancel'), href='/'),
                        html.Br(), html.Br()],
                        id='generate-content',
                        className="container")
                ])
            ])

        @app.callback(
            Output('generate-content', 'children'),
            [Input('button-generate-submit', 'n_clicks')],
            [State('dropdown-generate-proj', 'value'),
             State('dropdown-generate-assr-types', 'value'),
             State('dropdown-generate-scan-types', 'value'),
             State('dropdown-generate-prefix', 'value')])
        def handle_submit(n_clicks, pvalue, avalue, svalue, prefix):
            if not n_clicks:
                raise dash.exceptions.PreventUpdate("No data changed!")

            # Write script
            nowtime = datetime.now()
            ftime = datetime.strftime(nowtime, '%Y%m%d-%H%M%S')
            filebase = '{}_{}'.format(prefix, ftime)
            newdata_file = os.path.join(self.datadir, filebase + '.json')
            script_file = os.path.join(self.datadir, filebase + '.py')
            log_file = os.path.join(self.datadir, filebase + '.log')

            print('DEBUG:newdata_file=' + newdata_file)
            module_dir = os.path.abspath(os.path.join(
                os.path.dirname(__file__), '..'))
            script_text = self.GEN_TEMPLATE.format(
                module_dir, pvalue, avalue, svalue, newdata_file)
            with open(script_file, 'w') as f:
                f.writelines(script_text)

            # Write report in background
            cmd = 'python -u {} > {} 2>&1'.format(script_file, log_file)
            print(cmd)
            subprocess.Popen(cmd, shell=True)

            # Display message to user
            msg = 'Your report is being generated'
            msg += ' and will be available ASAP!'

            return [
                html.H3(msg),
                dcc.Link(html.Button('Go Back to Reports'), href='/'),
                dcc.Interval(
                    id='interval-log',
                    interval=5000,
                    n_intervals=0
                ),
                html.Br(),
                html.P(
                    children=['Loading log...'],
                    id='content-log',
                    style={'white-space': 'pre-wrap'})]

        @app.callback(
            Output('content-log', 'children'),
            [Input('interval-log', 'n_intervals')])
        def update_log_content(n):
            # Get list of log files sorted by descending modified time
            file_list = glob(self.datadir + '/*.log')
            file_list.sort(key=os.path.getmtime, reverse=True)

            if not file_list:
                raise dash.exceptions.PreventUpdate("No data changed!")

            log_file = file_list[0]
            print('DEBUG:reading file:' + log_file)
            with open(log_file, 'r') as f:
                content = f.read()

            return content

        @app.callback(
            Output('report-content', 'children'),
            [Input('dropdown-report-list', 'value')])
        def load_report_layout(selected_rpt):
            if selected_rpt is None:
                return 'No existing reports'

            print('DEBUG:load_report_layout')
            if self.dashdata.datafile != selected_rpt:
                print('DEBUG:load_report_layout:loading:' + selected_rpt)
                self.dashdata.load_data(selected_rpt)
                print('DEBUG:load_report_layout:finished loading')

            report_content = [
                html.Div(
                    dcc.Tabs(
                        id='tabs', value=1, children=[
                            dcc.Tab(label='Processing', value=1),
                            dcc.Tab(label='Jobs', value=2),
                            dcc.Tab(label='fMRIQA', value=3),
                            dcc.Tab(label='EDATQA', value=4),
                            dcc.Tab(label='Scans', value=5),
                            dcc.Tab(label='LST', value=6),
                            dcc.Tab(label='test', value=7),
                        ],
                        vertical=False
                    ),
                ),
                html.Div(id='tab-output'),
                html.Div(dt.DataTable(rows=[{}]), style={'display': 'none'})
            ]
            print('DEBUG:returning content')
            return report_content

        @app.callback(
            Output('tab-output', 'children'),
            [Input('tabs', 'value')])
        def display_content(value):
            if not self.dashdata.updatetime:
                return html.H3('No data yet')

            if value == 2:
                _df = self.dashdata.task_df
                task_proj_options = self.make_options(_df.project.unique())
                task_proc_options = self.make_options(_df.proctype.unique())

                return html.Div([
                    dcc.Graph(id='graph-task'),
                    dcc.Dropdown(id='dropdown-task-time', options=[
                        {'label': '1 day', 'value': 0},
                        {'label': '1 week', 'value': 1},
                        {'label': '1 month', 'value': 2},
                        {'label': '1 year', 'value': 3},
                        {'label': 'All time', 'value': 4}], value=2),
                    dcc.Dropdown(
                        id='dropdown-task-proj', multi=True,
                        options=task_proj_options, placeholder='All projects'),
                    dcc.Dropdown(
                        id='dropdown-task-proc', multi=True,
                        options=task_proc_options,
                        placeholder='All processing types'),
                    dt.DataTable(
                        rows=self.dashdata.task_df.to_dict('records'),
                        columns=self.TASK_COLS,
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        id='datatable-task'),
                    html.Div(id='selected-indexes')
                ], className="container", style={
                    'width:': '100%', 'max-width': 'none'})
            elif value == 3:
                _df = self.dashdata.fmri_df
                fmri_proj_options = self.make_options(_df.project.unique())
                fmri_type_options = self.make_options(_df.scan_type.unique())
                fmri_stat_options = self.make_options(_df.qcstatus.unique())
                fmri_cols = [
                    'label', 'project', 'session', 'qcstatus', 'scan_type',
                    'displace_median',
                    'displace_95',
                    'displace_99',
                    'sig_delta_95',
                    'tsnr_95',
                    'tsnr_median'
                ]
                return html.Div([
                    dcc.Graph(
                        id='graph-fmri'),
                    dcc.Dropdown(
                        id='dropdown-fmri-proj', multi=True,
                        options=fmri_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-fmri-type', multi=True,
                        options=fmri_type_options,
                        placeholder='Select scan type(s)'),
                    dcc.Dropdown(
                        id='dropdown-fmri-stat', multi=True,
                        options=fmri_stat_options,
                        placeholder='Select qc status'),
                    dt.DataTable(
                        rows=self.dashdata.fmri_df.to_dict('records'),
                        columns=fmri_cols,  # specifies order of columns
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        id='datatable-fmri'),
                    html.Div(id='selected-indexes-fmri'),
                ], className="container", style={"max-width": "none"})
            elif value == 4:
                _df = self.dashdata.edat_df
                edat_proj_options = self.make_options(_df.project.unique())
                edat_type_options = self.make_options(_df.scan_type.unique())
                edat_stat_options = self.make_options(_df.qcstatus.unique())
                edat_cols = [
                    'label', 'project', 'session', 'qcstatus', 'scan_type',
                    'acc_mean', 'rt_mean', 'trial_count'
                ]
                return html.Div([
                    dcc.Graph(
                        id='graph-edat'),
                    dcc.Dropdown(
                        id='dropdown-edat-proj', multi=True,
                        options=edat_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-edat-type', multi=True,
                        options=edat_type_options,
                        placeholder='Select scan type(s)'),
                    dcc.Dropdown(
                        id='dropdown-edat-stat', multi=True,
                        options=edat_stat_options,
                        placeholder='Select qc status'),
                    dt.DataTable(
                        rows=self.dashdata.edat_df.to_dict('records'),
                        columns=edat_cols,  # specifies order of columns
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        id='datatable-edat'),
                    html.Div(id='selected-indexes-edat'),
                ], className="container", style={"max-width": "none"})
            elif value == 5:
                _df = self.dashdata.scan_df
                scan_proj_options = self.make_options(_df.project.unique())
                scan_type_options = self.make_options(_df.type.unique())
                scan_stat_options = self.make_options(
                    ['Passed', 'Needs QA', 'Failed'])
                scan_cols = ['session', 'project'] + list(_df.type.unique())

                return html.Div([
                    dcc.Graph(
                        id='graph-scan'),
                    dcc.Dropdown(
                        id='dropdown-scan-proj', multi=True,
                        options=scan_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-scan-type', multi=True,
                        options=scan_type_options,
                        placeholder='Select scan type(s)'),
                    dcc.Dropdown(
                        id='dropdown-scan-stat', multi=True,
                        options=scan_stat_options,
                        placeholder='Select status'),
                    dt.DataTable(
                        rows=self.dashdata.scan_dfp.to_dict('records'),
                        columns=scan_cols,  # specifies order of columns
                        filterable=True,
                        sortable=True,
                        editable=False,
                        id='datatable-scan'),
                    html.Div(id='selected-indexes-scan'),
                ], className="container", style={"max-width": "none"})
            elif value == 6:
                _df = self.dashdata.lst_df
                lst_proj_options = self.make_options(_df.project.unique())
                lst_stat_options = self.make_options(_df.qcstatus.unique())
                lst_cols = [
                    'label', 'project', 'session', 'qcstatus', 'wml_volume']

                return html.Div([
                    dcc.Graph(
                        id='graph-lst'),
                    dcc.Dropdown(
                        id='dropdown-lst-proj', multi=True,
                        options=lst_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-lst-stat', multi=True,
                        options=lst_stat_options,
                        placeholder='Select qc status'),
                    dt.DataTable(
                        rows=self.dashdata.lst_df.to_dict('records'),
                        columns=lst_cols,  # specifies order of columns
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        id='datatable-lst'),
                    html.Div(id='selected-indexes-lst'),
                ], className="container", style={"max-width": "none"})
            elif value == 1:
                _df = self.dashdata.assr_df
                assr_proj_options = self.make_options(_df.project.unique())
                assr_type_options = self.make_options(_df.proctype.unique())
                acols = ['session', 'project'] + list(_df.proctype.unique())

                return html.Div([
                    dcc.Graph(
                        id='graph-both'),
                    dcc.Dropdown(
                        id='dropdown-both-proj', multi=True,
                        options=assr_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-both-type', multi=True,
                        options=assr_type_options,
                        placeholder='Select type(s)'),
                    dt.DataTable(
                        rows=self.dashdata.assr_dfp.to_dict('records'),
                        columns=acols,  # specifies order of columns
                        filterable=False,
                        sortable=True,
                        editable=False,
                        id='datatable-both'),
                    html.A(
                        html.Button('CSV'),
                        id='download-link',
                        download="assrdata.csv",
                        href="",
                        target="_blank")
                ], className="container", style={"max-width": "none"})

            elif value == 7:
                _df = self.dashdata.test_df
                test_proj_options = self.make_options(_df.project.unique())
                test_type_options = self.make_options(_df.proctype.unique())
                tcols = ['session', 'project'] + list(_df.proctype.unique())

                return html.Div([
                    dcc.Graph(
                        id='graph-test'),
                    dcc.Dropdown(
                        id='dropdown-test-proj', multi=True,
                        options=test_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-test-type', multi=True,
                        options=test_type_options,
                        placeholder='Select type(s)'),
                    dt.DataTable(
                        rows=self.dashdata.test_dfp.to_dict('records'),
                        columns=tcols,  # specifies order of columns
                        filterable=False,
                        sortable=True,
                        editable=False,
                        id='datatable-test'),
                    html.A(
                        html.Button('CSV'),
                        id='download-link',
                        download="testdata.csv",
                        href="",
                        target="_blank")
                ], className="container", style={"max-width": "none"})

        @app.callback(
            Output('datatable-task', 'rows'),
            [Input('dropdown-task-time', 'value'),
             Input('dropdown-task-proj', 'value'),
             Input('dropdown-task-proc', 'value')])
        def update_rows(selected_time, selected_proj, selected_proc):
            DFORMAT = self.DFORMAT
            starttime = self.dashdata.updated_datetime()
            task_df = self.dashdata.task_df

            # Filter by time
            if selected_time == 0:
                _prevtime = starttime - timedelta(days=1)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 1:
                _prevtime = starttime - timedelta(days=7)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 2:
                _prevtime = starttime - timedelta(days=30)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 3:
                _prevtime = starttime - timedelta(days=365)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            else:
                fdate = '1969-12-31'

            dff = task_df[(task_df['datetime'] > fdate)]

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by proctype
            if selected_proc:
                dff = dff[dff['proctype'].isin(selected_proc)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-task', 'figure'),
            [Input('datatable-task', 'rows'),
             Input('datatable-task', 'selected_row_indices')])
        def update_figure(rows, selected_row_indices):
            # Make dict of colors by status
            stat2color = {
                'COMPLETE': 'rgba(0,255,0,0.5)',
                'JOB_FAILED': 'rgba(255,0,0,0.5)',
                'JOB_RUNNING': 'rgba(0,0,255,0.5)',
                'UPLOADING': 'rgba(255,0,255,0.5)'}

            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Plot trace for each status
            for i in dff.procstatus.unique():
                # Filter data by status
                dft = dff[dff.procstatus == i]

                # Match status to main color
                try:
                    color = stat2color[i]
                except KeyError:
                    color = 'rgba(0,0,0,0.5)'

                # Line color
                line = 'rgba(50,50,50,0.9)'

                # Add trace to figure
                fig.append_trace({
                    'name': '{} ({})'.format(i, len(dft)),
                    'x': dft['datetime'],
                    'y': dft['timeused(min)'],
                    'text': dft['label'],
                    'mode': 'markers',
                    'marker': dict(
                        color=color, size=10, line=dict(width=1, color=line))
                }, 1, 1)

            # Customize figure
            fig['layout'].update(
                yaxis=dict(type='log', title='minutes used'),
                hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('graph-fmri', 'figure'),
            [Input('datatable-fmri', 'rows'),
             Input('datatable-fmri', 'selected_row_indices')])
        def update_figure_fmri(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x4 figure
            fig = plotly.tools.make_subplots(rows=1, cols=4)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Add traces to figure
            fig.append_trace(
                go.Box(
                    y=dff.displace_95,
                    name='displace_95',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 1)

            fig.append_trace(
                go.Box(
                    y=dff.displace_median,
                    name='displace_median',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 2)

            fig.append_trace(
                go.Box(
                    y=dff.displace_99,
                    name='displace_99',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 3)

            fig.append_trace(
                go.Box(
                    y=dff.sig_delta_95,
                    name='sig_95',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 4)

            # Customize figure
            fig['layout'].update(hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('datatable-fmri', 'rows'),
            [Input('dropdown-fmri-proj', 'value'),
             Input('dropdown-fmri-type', 'value'),
             Input('dropdown-fmri-stat', 'value')])
        def update_rows_fmri(selected_proj, selected_type, selected_stat):
            dff = self.dashdata.fmri_df

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by scan type
            if selected_type:
                dff = dff[dff['scan_type'].isin(selected_type)]

            # Filter by status
            if selected_stat:
                dff = dff[dff['qcstatus'].isin(selected_stat)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-scan', 'figure'),
            [Input('datatable-scan', 'rows'),
             Input('datatable-scan', 'selected_row_indices')])
        def update_figure_scan(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            ydata = dff.sort_values(
                'project').groupby('project')['session'].count()

            fig.append_trace(
                go.Bar(
                    x=sorted(dff.project.unique()),
                    y=ydata,
                    name='counts',
                ), 1, 1)

            return fig

        @app.callback(
            Output('datatable-scan', 'rows'),
            [Input('dropdown-scan-proj', 'value'),
             Input('dropdown-scan-type', 'value'),
             Input('dropdown-scan-stat', 'value')])
        def update_rows_scan(selected_proj, selected_type, selected_stat):
            dff = self.dashdata.scan_dfp

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            if selected_type:
                for t in selected_type:
                    # Filter to include anything with at least one P or Q
                    dff = dff[(dff[t].str.contains(
                        'P', na=False)) | (dff[t].str.contains('Q', na=False))]

            return dff.to_dict('records')

        @app.callback(
            Output('datatable-both', 'rows'),
            [Input('dropdown-both-proj', 'value'),
             Input('dropdown-both-type', 'value')])
        def update_rows_both(selected_proj, selected_type):
            dff = self.dashdata.assr_dfp

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-both', 'figure'),
            [Input('datatable-both', 'rows'),
             Input('dropdown-both-proj', 'value'),
             Input('dropdown-both-type', 'value')])
        def update_figure_both(rows, selected_proj, selected_type):
            dfp = pd.DataFrame(rows)
            xall = sorted(dfp.project.unique())
            yall = dfp.sort_values(
                'project').groupby('project')['session'].count()

            assr_proj_options = [{'label': x, 'value': x} for x in xall]

            yred = [0] * len(assr_proj_options)
            ygreen = [0] * len(assr_proj_options)
            ygrey = [0] * len(assr_proj_options)
            yyell = [0] * len(assr_proj_options)

            # Make a 1x1 figured
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            if not selected_type:
                # Draw bar
                fig.append_trace(go.Bar(
                    x=xall, y=yall, name='All',
                    marker=dict(color='rgb(59,89,152)')
                ), 1, 1)

            else:
                # TODO: optimize this cause it's ugly
                for i, proj in enumerate(xall):
                    # Subset for this project
                    dfpp = dfp.loc[dfp['project'] == proj]
                    # Iterate by session
                    for s, sess in dfpp.iterrows():
                        cur = 0
                        for t in selected_type:
                            if not sess[t]:
                                cur = 3
                                break
                            elif 'F' in sess[t] and cur <= 2:
                                cur = 2
                            elif (('Q' in sess[t] or 'J' in sess[t]) and
                                    cur < 1):
                                cur = 1

                        # Record final value across all types
                        if cur == 3:
                            ygrey[i] += 1
                        elif cur == 2:
                            yred[i] += 1
                        elif cur == 1:
                            yyell[i] += 1
                        else:
                            ygreen[i] += 1

                # Draw bar for each status
                fig.append_trace(go.Bar(
                    x=xall, y=ygreen, name='Passed',
                    marker=dict(color='rgb(27,157,5)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=yyell, name='Needs QA',
                    marker=dict(color='rgb(240,240,30)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=yred, name='Failed',
                    marker=dict(color='rgb(200,0,0)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=ygrey, name='None',
                    marker=dict(color='rgb(200,200,200)'),
                    opacity=0.9), 1, 1)

                # Customize figure
                fig['layout'].update(barmode='stack', showlegend=True)

            return fig

        @app.callback(
            Output('graph-lst', 'figure'),
            [Input('datatable-lst', 'rows'),
             Input('datatable-lst', 'selected_row_indices')])
        def update_figure_lst(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Add traces to figure
            fig.append_trace(
                go.Box(
                    y=dff.wml_volume,
                    name='wml_volume',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 1)

            # Customize figure
            fig['layout'].update(hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('datatable-lst', 'rows'),
            [Input('dropdown-lst-proj', 'value'),
             Input('dropdown-lst-stat', 'value')])
        def update_rows_lst(selected_proj, selected_stat):
            dff = self.dashdata.lst_df

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by status
            if selected_stat:
                dff = dff[dff['qcstatus'].isin(selected_stat)]

            return dff.to_dict('records')

        @app.callback(
            Output('download-link', 'href'),
            [Input('datatable-both', 'rows')])
        def update_download_link(rows):
            dff = pd.DataFrame(rows)
            _csv = dff.to_csv(index=False, encoding='utf-8')
            _csv = urllib.parse.quote(_csv)
            _csv = "data:text/csv;charset=utf-8,%EF%BB%BF" + _csv
            return _csv

        @app.callback(
            Output('update-text', 'children'),
            [Input('update-button', 'n_clicks')])
        def update_button_click(n_clicks):
            print('update_button_click', n_clicks)
            if n_clicks > 0:
                print('INFO:UPDATING DATA')
                self.update_data()

            return ['{}    '.format(self.dashdata.updatetime)]

        @app.callback(
            Output('graph-edat', 'figure'),
            [Input('datatable-edat', 'rows'),
             Input('datatable-edat', 'selected_row_indices')])
        def update_figure_edat(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x3 figure
            fig = plotly.tools.make_subplots(rows=1, cols=3)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Add traces to figure
            fig.append_trace(
                go.Box(
                    y=dff.acc_mean,
                    name='acc_mean',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 1)

            fig.append_trace(
                go.Box(
                    y=dff.rt_mean,
                    name='rt_mean',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 2)

            fig.append_trace(
                go.Box(
                    y=dff.trial_count,
                    name='trial_count',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 3)

            # Customize figure
            fig['layout'].update(hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('datatable-edat', 'rows'),
            [Input('dropdown-edat-proj', 'value'),
             Input('dropdown-edat-type', 'value'),
             Input('dropdown-edat-stat', 'value')])
        def update_rows_edat(selected_proj, selected_type, selected_stat):
            dff = self.dashdata.edat_df

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by scan type
            if selected_type:
                dff = dff[dff['scan_type'].isin(selected_type)]

            # Filter by status
            if selected_stat:
                dff = dff[dff['qcstatus'].isin(selected_stat)]

            return dff.to_dict('records')

        @app.callback(
            Output('datatable-test', 'rows'),
            [Input('dropdown-test-proj', 'value'),
             Input('dropdown-test-type', 'value')])
        def update_rows_test(selected_proj, selected_type):
            dff = self.dashdata.test_dfp

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-test', 'figure'),
            [Input('datatable-test', 'rows'),
             Input('dropdown-test-proj', 'value'),
             Input('dropdown-test-type', 'value')])
        def update_figure_test(rows, selected_proj, selected_type):
            _dfp = pd.DataFrame(rows)
            _df = self.dashdata.task_df

            if not selected_type:
                xall = list(_df.proctype.unique())
            else:
                xall = selected_type

            proctype_options = [{'label': x, 'value': x} for x in xall]
            yred = [0] * len(proctype_options)
            ygreen = [0] * len(proctype_options)
            ygrey = [0] * len(proctype_options)
            yyell = [0] * len(proctype_options)
            yblue = [0] * len(proctype_options)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)
            for i, t in enumerate(xall):
                # Iterate by session
                for s, sess in _dfp.iterrows():
                    if not sess[t]:
                        ygrey[i] += 1
                    elif 'P' in sess[t]:
                        ygreen[i] += 1
                    elif 'Q' in sess[t]:
                        yyell[i] += 1
                    elif 'J' in sess[t]:
                        yblue[i] += 1
                    elif 'F' in sess[t]:
                        yred[i] += 1
                    else:
                        ygrey[i] += 1

            # Draw bar for each status
            fig.append_trace(go.Bar(
                x=xall, y=ygreen, name='Passed',
                marker=dict(color='rgb(27,157,5)'),
                opacity=0.9), 1, 1)

            fig.append_trace(go.Bar(
                x=xall, y=yyell, name='Needs QA',
                marker=dict(color='rgb(240,240,30)'),
                opacity=0.9), 1, 1)

            fig.append_trace(go.Bar(
                x=xall, y=yred, name='Failed',
                marker=dict(color='rgb(200,0,0)'),
                opacity=0.9), 1, 1)

            fig.append_trace(go.Bar(
                x=xall, y=yblue, name='In Progress',
                marker=dict(color='rgb(65,105,225)'),
                opacity=0.9), 1, 1)

            fig.append_trace(go.Bar(
                x=xall, y=ygrey, name='None',
                marker=dict(color='rgb(200,200,200)'),
                opacity=0.9), 1, 1)

            # Customize figure
            fig['layout'].update(barmode='stack', showlegend=True)

            return fig

    def script_running(self):
        script_list = glob(self.datadir + '/*.py')
        for script in script_list:
            data_file = os.path.basename(script) + '.json'
            if not os.path.exists(data_file):
                return script

        return None

    def get_app(self):
        return self.app

    def run(self, host='0.0.0.0'):
        print('DEBUG:running app on host:' + host)
        self.app.run_server(host=host)
