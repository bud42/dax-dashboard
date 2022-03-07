import os
import yaml

# TODO: instead of a VAR_LIST, allow a subset filter for each proctype,
# so that by default we include everything, unless there's a subset filter.
# then we don't need to update this list for new stuff.


VAR_LIST = [
    'accuracy', 'RT', 'trials',  # EDATQA
    'WML',  # LST
    'VOXD', 'DVARS',  # fmriqa
    'compgm_suvr',  # amyvidqa
    'ETIV', 'LHPC', 'LVENT', 'LSUPFLOBE',  # FS6
    'bag_age_pred']  # BAG


STATS_RENAME = {
    'experiment': 'SESSION',
    'proctype': 'TYPE',
    'project': 'PROJECT',
    'proc_date': 'DATE',
    'stats_edatqa_acc_mean': 'accuracy',
    'stats_edatqa_rt_mean': 'RT',
    'stats_edatqa_trial_count': 'trials',
    'lst_stats_wml_volume': 'WML',
    'fmriqa_stats_wide_voxel_displacement_mm_95prctile': 'VOXD',
    'fmriqa_stats_wide_dvars_mean': 'DVARS',
    'stats_recon_estimatedtotalintracranialvol_etiv': 'ETIV'}


REDCAP_FILE = os.path.join(
    os.path.expanduser("~"),
    'redcap.yaml')

STATIC_COLUMNS = [
    'assessor_label',
    'PROJECT',
    'SESSION',
    'SUBJECT',
    'AGE',
    'SEX',
    'DEPRESS',
    'TYPE',
    'SITE',
    'SESSTYPE']


# check for a statsparams.yaml file to override var lists
try:
    # Read inputs yaml as dictionary
    PARAMSFILE = os.path.join(os.path.expanduser("~"), 'statsparams.yaml')
    with open(PARAMSFILE, 'rt') as file:
        print('loading stats params from file')
        params_data = yaml.load(file, yaml.SafeLoader)

        if 'VAR_LIST' in params_data:
            print('setting VAR_LIST')
            VAR_LIST = params_data['VAR_LIST']

        if 'STATS_RENAME' in params_data:
            print('setting STATS_RENAME')
            STATS_RENAME = params_data['STATS_RENAME']

except EnvironmentError:
    print('params file not found, not loading')


# check for a demog yaml file (this can be mounted into container in home)
try:
    # Read inputs yaml as dictionary
    DEMOG_FILE = os.path.join(os.path.expanduser("~"), 'demogparams.yaml')
    with open(DEMOG_FILE, 'rt') as file:
        print('loading demographics keys from file')
        _data = yaml.load(file, yaml.SafeLoader)
        DEMOG_KEYS = _data['api_keys']
        REDCAP_URL = _data['api_url']

except EnvironmentError:
    print('demographics keys not found, not loading')
