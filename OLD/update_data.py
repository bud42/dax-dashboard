from dax import XnatUtils
import pandas as pd
from datetime import datetime
import os
import yaml
import argparse


class DaxDashboardUpdater:
    def __init__(self, config_file):
        self.config = None

        with open(config_file, 'r') as f:
            self.config = yaml.load(f)

        if not os.path.exists(self.config['data_dir']):
            raise IOError('Dir does not exist:' + self.config['data_dir'])

    def update(self):
        SCAN_COLUMNS = [
            'quality',
            'type',
            'session_label',
            'project_id']

        SCAN_TYPE_LIST = [
            'T1', 'FLAIR', 'fMRI_Resting', 'T2_HPC',
            'fMRI_Posner', 'fMRI_EDP', 'fMRI_EmoStroop', 'fMRI_NBack',
            'fMRI_STMP', 'fMRI_Flanker', 'fMRI_MIST', 'fMRI_Task',
            'DTI', 'DTI_AP', 'DTI_PA', 'WIP_dti_4min_matchTE',
            'WIP_HARDI_60_2_5iso']

        ASSR_TYPE_LIST = [
            'FS6_v1', 'ASHS_v1', 'LST_v1', 'RSFC_CONN_v1', 'TRACULA_v1',
            'EDATQA_v1',
            'dtiQA_v6', 'BrainAgeGap_v1', 'Multi_Atlas_v2', 'Temporal_lobe_v3',
            'RWML_v1', 'fMRIQA_v2', 'fMRIQA_v3', 'fMRIQA_v4']

        ROOTDIR = self.config['data_dir']

        FAV_URI = '/data/archive/projects?favorite=True'

        # Load data from XNAT
        xnat = XnatUtils.get_interface()

        if self.config['xnat_projects'] == 'favorites':
            # Load projects that have been checked as Favorite for current user
            print('Loading project list from XNAT')
            proj_list = [x['id'] for x in xnat._get_json(FAV_URI)]
            print(proj_list)
        else:
            print('invalid value for projects')
            return

        # Build list of scans/assrs for all projects
        scan_list = list()
        assr_list = list()
        for proj in proj_list:
            print('DEBUG:getting list of scans/assrs for project:' + proj)
            scan_list.extend(xnat.get_project_scans(proj))
            assr_list.extend(XnatUtils.list_project_assessors(xnat, proj))

        # Transform data

        # Convert scan to dataframe and filter it
        scan_df = pd.DataFrame(scan_list)
        scan_df = scan_df[scan_df['type'].isin(SCAN_TYPE_LIST)]
        scan_df['quality'].replace(
            {'usable': 'P', 'unusable': 'F', 'questionable': 'Q'},
            inplace=True)
        scan_df = scan_df[SCAN_COLUMNS]

        # Convert assr to dataframe and filter it
        assr_df = pd.DataFrame(assr_list)
        assr_df = assr_df[assr_df['proctype'].isin(ASSR_TYPE_LIST)]
        assr_df['qcstatus'].replace({
            'Passed': 'P', 'passed': 'P',
            'Questionable': 'P',
            'Failed': 'F',
            'Needs QA': 'Q'},
            inplace=True)
        assr_df.loc[(assr_df['qcstatus'].str.len() > 1), 'qcstatus'] = 'J'
        assr_df.qcstatus.unique()

        # Get the time for naming files
        curtime = datetime.strftime(datetime.now(), '%Y%m%d-%H%m%S')

        # Write scan file
        scan_file = ROOTDIR + '/scandata-' + curtime + '.csv'
        print('writing:' + scan_file)
        scan_df.to_csv(scan_file)

        # Write assr file
        assr_file = ROOTDIR + '/assrdata-' + curtime + '.csv'
        print('writing:' + assr_file)
        assr_df.to_csv(assr_file)

        # Write squeue file
        if self.config['run_squeue']:
            _user = self.config['squeue_user']
            _file = ROOTDIR + '/squeue-' + curtime + '.txt'
            cmd = 'squeue -u ' + _user + ' --format="%all" > ' + _file
            print('running:' + cmd)
            os.system(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    args = parser.parse_args()
    print(args)

    config_file = args.config

    if not config_file:
        config_file = 'config.yaml'

    print(config_file)

    updater = DaxDashboardUpdater(config_file)

    print(updater.config)

    updater.update()

    print('DONE')
