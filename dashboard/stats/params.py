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
    'hpcwhole_lh', 'hpchead_lh', 'hpcbody_lh', 'hpctail_lh', # FS7HPCAMYG
    'hpcwhole_rh', 'hpchead_rh', 'hpcbody_rh', 'hpctail_rh',  # FS7HPCAMYG
    'amgwhole_lh', 'amglatnuc_lh', 'amgbasnuc_lh', 'amgaccnuc_lh',
    'amgwhole_rh', 'amglatnuc_rh', 'amgbasnuc_rh', 'amgaccnuc_rh',
    'nucleusaccumbens_lh', 'nucleusaccumbens_rh',  # FS7sclimbic
    'hypothalnomb_lh', 'hypothalnomb_rh',   # FS7sclimbic
    'fornix_lh', 'fornix_rh',   # FS7sclimbic
    'mammbody_lh', 'mammbody_rh',   # FS7sclimbic
    'basalforebrain_lh', 'basalforebrain_rh',   # FS7sclimbic
    'septalnuc_lh', 'septalnuc_rh',
    'bfc_ch4_lh_gmd', 'bfc_ch4_rh_gmd', 'bfc_ch123_lh_gmd', 'bfc_ch123_rh_gmd',
    'bfc_ch4_lh_vol', 'bfc_ch4_rh_vol', 'bfc_ch123_lh_vol', 'bfc_ch123_rh_vol',
    'totgm_vol', 'totwm_vol', 'totcsf_vol',
    'compositegm_suvr',
    'shen268_thr0p1_clustcoeff',
    'shen268_thr0p1_localeff',
    'shen268_thr0p1_globaleff',
    'shen268_thr0p3_clustroeff',
    'shen268_thr0p3_localeff',
    'shen268_thr0p3_globaleff',
    'schaefer400_thr0p1_clustcoeff',
    'schaefer400_thr0p1_localeff',
    'schaefer400_thr0p1_globaleff',
    'schaefer400_thr0p3_clustcoeff',
    'schaefer400_thr0p3_localeff',
    'schaefer400_thr0p3_globaleff']


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
    'sclimbic_volumes_all_leftnucleusaccumbens': 'nucleusaccumbens_lh',
    'sclimbic_volumes_all_rightnucleusaccumbens': 'nucleusaccumbens_rh',
    'sclimbic_volumes_all_lefthypothalnomb': 'hypothalnomb_lh',
    'sclimbic_volumes_all_righthypothalnomb': 'hypothalnomb_rh',
    'sclimbic_volumes_all_leftfornix': 'fornix_lh',
    'sclimbic_volumes_all_rightfornix': 'fornix_rh',
    'sclimbic_volumes_all_leftmammillarybody': 'mammbody_lh',
    'sclimbic_volumes_all_rightmammillarybody': 'mammbody_rh',
    'sclimbic_volumes_all_leftbasalforebrain': 'basalforebrain_lh',
    'sclimbic_volumes_all_rightbasalforebrain': 'basalforebrain_rh',
    'sclimbic_volumes_all_leftseptalnuc': 'septalnuc_lh',
    'sclimbic_volumes_all_rightseptalnuc': 'septalnuc_rh',
    'sclimbic_volumes_all_etiv': 'etiv',
    'stats_ch4_l_mean': 'bfc_ch4_lh_gmd',
    'stats_ch4_r_mean': 'bfc_ch4_rh_gmd',
    'stats_ch123_l_mean': 'bfc_ch123_lh_gmd',
    'stats_ch123_r_mean': 'bfc_ch123_rh_gmd',
    'stats_ch4_l_vol': 'bfc_ch4_lh_vol',
    'stats_ch4_r_vol': 'bfc_ch4_rh_vol',
    'stats_ch123_l_vol': 'bfc_ch123_lh_vol',
    'stats_ch123_r_vol': 'bfc_ch123_rh_vol',
    'stats_totgm_vol': 'totgm_vol',
    'stats_totwm_vol': 'totwm_vol',
    'stats_totcsf_vol': 'totcsf_vol',
    #'stats_schaefer200_thr0p1_clusteringcoeff': '',
    #'stats_schaefer200_thr0p1_degree': '',
    #'stats_schaefer200_thr0p1_evc': '',
    #'stats_schaefer200_thr0p1_pathlength': '',
    #'stats_schaefer200_thr0p1_localeff': '',
    #'stats_schaefer200_thr0p1_globaleff': '',
    #'stats_schaefer200_thr0p3_clusteringcoeff': '',
    #'stats_schaefer200_thr0p3_degree': '',
    #'stats_schaefer200_thr0p3_evc': '',
    #'stats_schaefer200_thr0p3_pathlength':'',
    #'stats_schaefer200_thr0p3_localeff':'',
    #'stats_schaefer200_thr0p3_globaleff': '',
    #'stats_aal3_thr0p1_clusteringcoeff,
    #'stats_aal3_thr0p1_degree':'',
    #'stats_aal3_thr0p1_evc':'',
    #stats_aal3_thr0p1_pathlength,
    #'stats_aal3_thr0p1_localeff':'',
    #'stats_aal3_thr0p1_globaleff':'',
    #'stats_aal3_thr0p3_clusteringcoeff':,
    #'stats_aal3_thr0p3_degree,
    #stats_aal3_thr0p3_evc,
    #'stats_aal3_thr0p3_pathlength,
    #'stats_aal3_thr0p3_localeff': '',
    #'stats_aal3_thr0p3_globaleff': '',
    'stats_shen268_thr0p1_clusteringcoeff': 'shen268_thr0p1_clustcoeff',
    #'stats_shen268_thr0p1_degree,
    #stats_shen268_thr0p1_evc,
    #'stats_shen268_thr0p1_pathlength,
    'stats_shen268_thr0p1_localeff': 'shen268_thr0p1_localeff',
    'stats_shen268_thr0p1_globaleff': 'shen268_thr0p1_globaleff',
    'stats_shen268_thr0p3_clusteringcoeff': 'shen268_thr0p3_clustcoeff',
    #stats_shen268_thr0p3_degree,
    #stats_shen268_thr0p3_evc,
    #'stats_shen268_thr0p3_pathlength,
    'stats_shen268_thr0p3_localeff': 'shen268_thr0p3_localeff',
    'stats_shen268_thr0p3_globaleff': 'shen268_thr0p3_globaleff',
    'stats_schaefer400_thr0p1_clusteringcoeff': 'schaefer400_thr0p1_clustcoeff',
    #'stats_schaefer400_thr0p1_degree':'',
    #'stats_schaefer400_thr0p1_evc,
    #stats_schaefer400_thr0p1_pathlength,
    'stats_schaefer400_thr0p1_localeff': 'schaefer400_thr0p1_localeff',
    'stats_schaefer400_thr0p1_globaleff': 'schaefer400_thr0p1_globaleff',
    'stats_schaefer400_thr0p3_clusteringcoeff': 'schaefer400_thr0p3_clustcoeff',
    #stats_schaefer400_thr0p3_degree,
    #stats_schaefer400_thr0p3_evc,
    #stats_schaefer400_thr0p3_pathlength,
    'stats_schaefer400_thr0p3_localeff': 'schaefer400_thr0p3_localeff',
    'stats_schaefer400_thr0p3_globaleff': 'schaefer400_thr0p3_globaleff',
}


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
