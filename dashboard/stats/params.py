import os
import yaml


# TODO: refactor this so we link vars to the type and allow filtering somewhere

VAR_LIST = [
    'ma_tot',
    'lesions', 'sbtiv', # SAMSEG/FS7
    'accuracy', 'RT', 'trials', # EDATQA
    'WML', # LST
    'VOXD', 'DVARS',  # fmriqa
    'compgm_suvr',  # amyvidqa
    'ETIV', 'HPC_lh', 'HPC_rh', 'LV_lh', 'LV_rh', 'SUPFLOBE_lh', 'SUPFLOBE_rh',  # FS6
    'bag_age_pred',  # BAG
    'con_amyg', 'inc_amyg', 'con_bnst', 'inc_bnst',  # MSIT
    'con_pvn', 'inc_pvn', 'con_sgacc', 'inc_sgacc',
    'con_lhpostins', 'inc_lhpostins', 'ART_lib_outliers',
    'Accuracy', 'RT_mean',  # emostroop
    'lhMFG1_incgtcon', 'lhMFG2_incgtcon', 'lhMFG3_incgtcon', 'lhSFG1_incgtcon', 'lhSFG2_incgtcon',
    'rhMFG1_incgtcon', 'rhMFG2_incgtcon', 'rhMFG3_incgtcon', 'rhSFG1_incgtcon', 'rhSFG2_incgtcon',
    'lhMFG1_pctused', 'lhMFG2_pctused', 'lhMFG3_pctused', 'lhSFG1_pctused', 'lhSFG2_pctused',
    'rhMFG1_pctused', 'rhMFG2_pctused', 'rhMFG3_pctused', 'rhSFG1_pctused', 'rhSFG2_pctused',
    'con_minus_inc_rt_mean', 'congruent_rt_mean', 'congruent_rt_median', 'incongruent_rt_mean', 'incongruent_rt_median',
    'etiv', 'stnv', 'hpc_lh', 'hpc_rh', 'latvent_lh', 'latvent_rh', 'supflobe_lh', 'supflobe_rh',
    'hpcwhole_lh', 'hpchead_lh', 'hpcbody_lh', 'hpctail_lh',
    'hpcwhole_rh', 'hpchead_rh', 'hpcbody_rh', 'hpctail_rh',
    'amgwhole_lh', 'amglatnuc_lh', 'amgbasnuc_lh', 'amgaccnuc_lh',
    'amgwhole_rh', 'amglatnuc_rh', 'amgbasnuc_rh', 'amgaccnuc_rh',
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
    'stats_recon_left_hippocampus_volume_mm3': 'HPC_lh',
    'stats_recon_right_hippocampus_volume_mm3': 'HPC_rh',
    'stats_recon_left_lateral_ventricle_volume_mm3': 'LV_lh',
    'stats_recon_right_lateral_ventricle_volume_mm3': 'LV_rh',
    'stats_recon_lh_superiorfrontal_thickavg': 'SUPFLOBE_lh',
    'stats_recon_rh_superiorfrontal_thickavg': 'SUPFLOBE_rh',
    'stats_bag_age_pred': 'bag_age_pred',
    'stats_con_amyg_mean': 'con_amyg',
    'stats_inc_amyg_mean': 'inc_amyg',
    'stats_con_bnst_mean': 'con_bnst',
    'stats_inc_bnst_mean': 'inc_bnst',
    'stats_con_lhpostins_mean': 'con_lhpostins',
    'stats_inc_lhpostins_mean': 'inc_lhpostins',
    'stats_con_pvn_mean': 'con_pvn',
    'stats_inc_pvn_mean': 'inc_pvn', 
    'stats_con_sgacc_mean': 'con_sgacc', 
    'stats_inc_sgacc_mean': 'inc_sgacc',
    'stats_lib_outlier_count': 'ART_lib_outliers',
    'stats_samseg_lesions': 'lesions',
    'stats_samseg_sbtiv': 'sbtiv',
    'stats_overall_acc': 'Accuracy',
    'stats_overall_rt_mean': 'RT_mean',
    'stats_lhmfg1_incgtcon': 'lhMFG1_incgtcon',
    'stats_lhmfg2_incgtcon': 'lhMFG2_incgtcon',
    'stats_lhmfg3_incgtcon': 'lhMFG3_incgtcon',
    'stats_lhsfg1_incgtcon': 'lhSFG1_incgtcon',
    'stats_lhsfg2_incgtcon': 'lhSFG2_incgtcon',
    'stats_rhmfg1_incgtcon': 'rhMFG1_incgtcon',
    'stats_rhmfg2_incgtcon': 'rhMFG2_incgtcon',
    'stats_rhmfg3_incgtcon': 'rhMFG3_incgtcon',
    'stats_rhsfg1_incgtcon': 'rhSFG1_incgtcon',
    'stats_rhsfg2_incgtcon': 'rhSFG2_incgtcon',
    'stats_lhmfg1_pctused': 'lhMFG1_pctused',
    'stats_lhmfg2_pctused': 'lhMFG2_pctused',
    'stats_lhmfg3_pctused': 'lhMFG3_pctused',
    'stats_lhsfg1_pctused': 'lhSFG1_pctused',
    'stats_lhsfg2_pctused': 'lhSFG2_pctused',
    'stats_rhmfg1_pctused': 'rhMFG1_pctused',
    'stats_rhmfg2_pctused': 'rhMFG2_pctused',
    'stats_rhmfg3_pctused': 'rhMFG3_pctused',
    'stats_rhsfg1_pctused': 'rhSFG1_pctused',
    'stats_rhsfg2_pctused': 'rhSFG2_pctused',
    'stats_con_minus_inc_rt_mean': 'con_minus_inc_rt_mean',
    'stats_congruent_rt_mean': 'congruent_rt_mean',
    'stats_congruent_rt_median': 'congruent_rt_median',
    'stats_incongruent_rt_mean': 'incongruent_rt_mean',
    'stats_incongruent_rt_median': 'incongruent_rt_median',
    'stats_fs7_etiv': 'etiv',
    'stats_fs7_stnv': 'stnv',
    'stats_fs7_hpc_lh': 'hpc_lh',
    'stats_fs7_hpc_rh': 'hpc_rh',
    'stats_fs7_latvent_lh': 'latvent_lh',
    'stats_fs7_latvent_rh': 'latvent_rh',
    'stats_fs7_supflobe_lh': 'supflobe_lh',
    'stats_fs7_supflobe_rh': 'supflobe_rh',
    'stats_hpcwhole_lh': 'hpcwhole_lh',
    'stats_hpchead_lh': 'hpchead_lh',
    'stats_hpcbody_lh': 'hpcbody_lh',
    'stats_hpctail_lh': 'hpctail_lh',
    'stats_hpcwhole_rh': 'hpcwhole_rh',
    'stats_hpchead_rh': 'hpchead_rh',
    'stats_hpcbody_rh': 'hpcbody_rh',
    'stats_hpctail_rh': 'hpctail_rh',
    'stats_amgwhole_lh': 'amgwhole_lh',
    'stats_amglatnuc_lh': 'amglatnuc_lh',
    'stats_amgbasnuc_lh': 'amgbasnuc_lh',
    'stats_amgaccnuc_lh': 'amgaccnuc_lh',
    'stats_amgwhole_rh': 'amgwhole_rh',
    'stats_amglatnuc_rh': 'amglatnuc_rh',
    'stats_amgbasnuc_rh': 'amgbasnuc_rh',
    'stats_amgaccnuc_rh': 'amgaccnuc_rh',
}



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
