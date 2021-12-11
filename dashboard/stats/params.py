import os

# TODO: instead of a VAR_LIST, allow a subset filter for each proctype,
# so that by default we include everything, unless there's a subset filter.
# then we don't need to update this list for new stuff.


VAR_LIST = [
    'accuracy', 'RT', 'trials', # EDATQA
    'WML', # LST
    'VOXD', 'DVARS',  # fmriqa
    'compgm_suvr'.  # amyvidqa
    'ETIV', 'LHPC', 'RHPC', 'LVENT', 'RVENT', 'LSUPFLOBE', 'RSUPFLOBE',  # FS6
    'bag_age_pred',  # BAG
    ]

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
    'stats_amyvid_compgm_suvr': 'compgm_suvr',
    'stats_recon_estimatedtotalintracranialvol_etiv': 'ETIV',
    'stats_recon_left_hippocampus_volume_mm3': 'LHPC',
    'stats_recon_right_hippocampus_volume_mm3': 'RHPC',
    'stats_recon_left_lateral_ventricle_volume_mm3': 'LVENT',
    'stats_recon_right_lateral_ventricle_volume_mm3': 'RVENT',
    'stats_recon_lh_superiorfrontal_thickavg': 'LSUPFLOBE',
    'stats_recon_rh_superiorfrontal_thickavg': 'RSUPFLOBE',
    'stats_bag_age_pred': 'bag_age_pred',}


REDCAP_FILE = os.path.join(
    os.path.expanduser("~"),
    'redcap.yaml')

DEFAULT_COLUMNS = [
    'assessor_label',
    'PROJECT',
    'SESSION',
    'TYPE']

# check for a statsparams.yaml file to override var lists
try:
    # Read inputs yaml as dictionary
    PARAMSFILE = os.path.join(os.path.expanduser("~"), 'statsparams.yaml')
    with open(PARAMSFILE, 'rt') as file:
        print('loading params from file')
        params_data = yaml.load(file, yaml.SafeLoader)
        VAR_LIST = params_data['VAR_LIST']
        STATS_RENAME = params_data['STATS_RENAME']
except EnvironmentError:
    print('params file not found, not loading')
