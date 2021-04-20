import os


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
    'stats_recon_rh_superiorfrontal_thickavg': 'RSUPFLOBE'}


REDCAP_FILE = os.path.join(
    os.path.expanduser("~"),
    'redcap.yaml')

DEFAULT_COLUMNS = [
    'assessor_label',
    'PROJECT',
    'SESSION',
    'TYPE']
