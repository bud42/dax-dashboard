ASSR_EXCLUDE_LIST = [
    'ASHS_v1',
    'ASHS_v2',
    'BrainAgeGap_v1',
    'FMRIQA_v4',
    'surf_postproc_v1',
    'surf_quant_stat_v1',
    'surf_quant_v1',
    'slant_v1',
    'LST_v2',
    'Multi_Atlas_v2',
    'RWML_v1',
    'fMRIQA_v3',
    'Temporal_Lobe_v3',
    'RSFC_CONN_v1',
    'dtiQA_v6',
    'dtiQA_BL3_v7'
]

SCAN_EXCLUDE_LIST = [
    'ARC_GRE_field_mapping',
    'ARC_GRE_field_mapping ',
    ' ARC_GRE_field_mapping ',
    '&amp;amp;lt;MPR Collection&amp;amp;gt;',
    '1mm+PSF_CTAC',
    '1mm_CTAC',
    '2mm+PSF_CTAC',
    '2mm-CTAC',
    '3 Plane Localizer',
    '32CH_v2 ARC_2D_pASL',
    '32CH_v2 DTI_AP',
    '32CH_v2 DTI_AP_ADC',
    '32CH_v2 DTI_AP_ColFA',
    '32CH_v2 DTI_AP_FA',
    '32CH_v2 DTI_AP_TENSOR',
    '32CH_v2 DTI_AP_TRACEW',
    '3DPCA_DEM_MJD',
    '4X5-1MM',
    '4X5-1MM+PSF',
    '4X5-2MM',
    '4X5-2MM+PSF',
    'AAHScout',
    'AAHScout_MPR_cor',
    'AAHScout_MPR_sag',
    'AAHScout_MPR_tra',
    'AAHead_Scout_32ch-head-coil',
    'AAHead_Scout_32ch-head-coil_MPR_cor',
    'AAHead_Scout_32ch-head-coil_MPR_sag',
    'AAHead_Scout_32ch-head-coil_MPR_tra',
    'ABCD_DTI_6dirs_FSA',
    'ABCD_DTI_96dirs_mixed_FSP',
    'ABCD_T1w_MPR_vNav',
    'ABCD_T1w_MPR_vNav_setter',
    'ABCD_dMRI_2mm',
    'ABCD_dMRI_DistortionMap_AP_2mm',
    'ABCD_dMRI_DistortionMap_PA_2mm',
    'ABCD_fMRI_DistortionMap_AP',
    'ABCD_fMRI_DistortionMap_PA',
    'ABCD_fMRI_rest_PMU',
    'ARC_2D_pASL',
    'ARC_GRE_field_mapping',
    'Ax',
    'Ax-B0map',
    'Axial 2D PASL',
    'Axial ASL rev3',
    'Axial DTI rev2_ADC',
    'Axial DTI rev2_ColFA',
    'Axial DTI rev2_FA',
    'Axial DTI rev2_TENSOR',
    'Axial DTI rev2_TRACEW',
    'Axial Field Map rev2',
    'Axial Field Map rev2 repeat',
    'Axial fMRI Resting State - eyes OPEN rev3 repeat',
    'AxialField Mapping',
    'AxialField Mapping2',
    'B0',
    'B0_Map',
    'B0_PreScan',
    'CT',
    'Cor',
    'DEFAULT PS SERIES',
    'DTI_AP_ADC',
    'DTI_AP_ColFA',
    'DTI_AP_FA',
    'DTI_AP_TENSOR',
    'DTI_AP_TRACEW',
    'FLAIR_spc_ir_sag_p2_',
    'GRE FIELD MAPPING',
    'Head-Low Dose CT',
    'Head-Low Dose CT, iDose (4)',
    'LOC',
    'Localizer',
    'Localizer_aligned',
    'MB2 fMRI Fieldmap A',
    'MB2 fMRI Fieldmap P',
    'MIP - 3DPCA_DEM_MJD',
    'MPR AX 3D T2',
    'MPR AX FLAIR',
    'MPR COR 3D T2',
    'MPR COR FLAIR 1.0mm',
    'MPRAGE 3D 1st',
    'MSIT_PMU',
    'MoCoSeries',
    'PD_T2',
    'PD_T2_TSE',
    'Patient Aligned MPR AWPLAN_SMARTPLAN_TYPE_BRAIN',
    'Perfusion_Weighted',
    'PhoenixZIPReport',
    'SCOUT',
    'SCOUT_WT_MJD',
    'SE-fMRI A',
    'SE-fMRI P',
    'SOURCE - WIP ASL_BASELINE PGPP_6800ms',
    'SURVEY',
    'Sag',
    'Sag MPRAGE T1',
    'SmartBrain',
    'Survey',
    'Survey SHC32',
    'Survey_32ch_HeadCoil',
    'THREE PLANE LOC',
    'Unknown',
    'VWIP 3D T1 AX',
    'VWIP 3D T1 Cor',
    'VWIP AnatBrain_T1W3D SENSE',
    'VWIP MPRAGE',
    'VWIP T1W/3D/TFE_SENSE',
    'VWIP T1W/3D/TFE_SENSE SENSE',
    'VWIP cs_T1W_3D_TFE_32ch_Fast_ax',
    'WIP MPR - SmartBrain',
    'WIP MSIT',
    'WIP SOURCE - pCASL_SoftSinglePhase',
    'WIP WIP WIP BOLDMB2_shift3_TR0.66 SENSE',
    'WIP rsEPI_MB6R1_FSA',
    'WIP_dti_32_1000_multiband2',
    'WIP_dti_32_1000_nomb',
    '[BR-DY_CTAC] 2MM Brain Dynamic',
    '[BR-DY_CTAC] 2mm',
    '[BR-DY_CTAC_2MM] Brain Dynamic',
    '[BR-DY_CTAC_2mm+PSF] Brain Dynamic',
    '[BR-DY_CTAC_2mm+PSF] Brain Dynamic (18)',
    '[BR-DY_CTAC_2mmPSF] Brain Dynamic',
    '[BR-DY_CTAC_2mm] Brain Dynamic',
    '[BR-DY_NAC] 2mm',
    '[BR-DY_NAC] Brain Dynamic',
    '[BR-DY_NAC_2MM+PSF] Brain Dynamic',
    '[BR-DY_NAC_2MM] Brain Dynamic',
    '[BR-DY_NAC_2mm+PSF] Brain Dynamic (18)',
    '[BR_CTAC_2mm+PSF] Brain Dynamic (16)',
    '[BR_CTAC_2mm+PSF] Brain Dynamic (17)',
    '[BR_CTAC_2mm+PSF] Brain Dynamic (18)',
    '[BR_CTAC_2mm+PSF] Brain Dynamic (19)',
    '[BR_CTAC_2mm] Brain Dynamic (17)',
    '[DY_CTAC_4mm+PSF] Brain Dynamic (22)',
    '[DY_CTAC_4mm-576FOV] Brain Dynamic (22)',
    '[DetailBR_CTAC_1mm+PSF] Brain Dynamic',
    '[DetailBR_CTAC_1mm+PSF] Brain Dynamic (16)',
    '[DetailBR_CTAC_1mm+PSF] Brain Dynamic (17)',
    '[DetailBR_CTAC_1mm+PSF] Brain Dynamic (18)',
    '[DetailBR_CTAC_1mm+PSF] Brain Dynamic (19)',
    '[DetailBR_CTAC_1mmPSF] Brain Dynamic',
    '[DetailBR_CTAC_1mm] Brain Dynamic',
    '[DetailBR_CTAC_1mm] Brain Dynamic (16)',
    '[DetailBR_CTAC_1mm] Brain Dynamic (17)',
    '[DetailBR_CTAC_1mm] Brain Dynamic (18)',
    '[DetailBR_CTAC_1mm] Brain Dynamic (19)',
    '[DetailBR_NAC_1MM] Brain Dynamic',
    '[DetailBR_CTAC_1MM] Brain Dynamic',
    '[Not_for_Clinical_Use-SuperWB_CTAC_BODY3-15] Brain Dynamic',
    '[PREVIEW] Brain Dynamic',
    '[research_only-BR-DY1mm_CTAC_1mm+PSF] Brain Dynamic',
    '[research_only-BR-DY1mm_CTAC_1mm] Brain Dynamic',
    '[research_only-BR-DY1mm_NAC_1MM+PSF] Brain Dynamic',
    '[research_only-BR-DY1mm_NAC_1MM] Brain Dynamic',
    'anatqa',
    'bl_dti32MB_2min',
    'bl_dti32MBapa_2min',
    'cor',
    'dtiqa',
    'ep2d_2D_pASL',
    'ep2d_bold_n-back rev2_repeat',
    'fs',
    'localizer_32ch',
    'localizer_32ch_ND',
    'pCASL_SoftSinglePhase',
    'ref',
    'relCBF',
    'rsfMRI_Run_#1_P/A',
    'unknown',
    'v2 ARC_GRE__field_mapping',
    'MultiPlanar Reconstruction (MPR) Ob_Ax_S -&amp;gt; I_Average_sp:1.0_th:',
    'MultiPlanar Reconstruction (MPR) Ob_Sag_R -&amp;gt; L_Average_sp:1.0_th',
    'MultiPlanar Reconstruction (MPR) Ob_Cor_A -&amp;gt; P_Average_sp:1.0_th',
    'MultiPlanar Reconstruction (MPR) Ob_Sag_R -&amp;amp;amp;amp;amp;amp;gt; L_Average_sp:1.0_th',
    'MultiPlanar Reconstruction (MPR) Ob_Ax_S -&amp;amp;amp;amp;amp;amp;gt; I_Average_sp:1.0_th:',
    'MultiPlanar Reconstruction (MPR) Ob_Cor_A -&amp;amp;amp;amp;amp;amp;gt; P_Average_sp:1.0_th',
    'MultiPlanar Reconstruction (MPR) Ob_Ax_S -&gt; I_Average_sp:1.0_th:',
    'MAP_B0_2.4mm_96',
    'ORIG STRUC_MPRAGE_noPROMO_ABCD',
    'ORIG STRUC_T2FLAIRCube',
    'fMRI_posner',
    'FUNC_TOPUP_FPE',
    'FUNC_TOPUP_RPE',
    'WIP_HARDI_60_2_5iso',
    'T2_COR_HIPPO',
    'WIP_dti_4min_matchTE',
    'RESTING STATE fMRI',
    'DTI',
    'DTI_PA',
    'LOC_3-P_FGRE']
