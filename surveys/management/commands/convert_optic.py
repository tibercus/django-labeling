from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime  # for datetime64[ns] format
import numpy as np

import pickle
import pyarrow as pa
import pyarrow.parquet as pq

from django.conf import settings
import os

# from surveys.models import *


class Command(BaseCommand):
    help = "Convert Optical sources from PKL to Parquet file."

    def add_arguments(self, parser):
        parser.add_argument('survey_num', type=int, help='number of survey')

    base_fields = ['srcname_fin', 'RA_fin', 'DEC_fin', 'hpidx', 'opt_id', 'opt_hpidx', 'survey', 'file_name']

    @staticmethod
    def get_ls_fields():
        fields = ['srcname_fin', 'hpidx', 'opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'flag_agn_wise', 'star',
                  'brick_primary', 'maskbits', 'fitbits', 'type', 'ra_ivar', 'dec_ivar', 'bx', 'by', 'ebv',
                  'mjd_min', 'mjd_max', 'ref_cat', 'ref_id', 'pmra', 'pmdec', 'parallax', 'pmra_ivar', 'pmdec_ivar',
                  'parallax_ivar', 'ref_epoch', 'gaia_phot_g_mean_mag', 'gaia_phot_g_mean_flux_over_error', 'gaia_phot_g_n_obs',
                  'gaia_phot_bp_mean_mag', 'gaia_phot_bp_mean_flux_over_error', 'gaia_phot_bp_n_obs', 'gaia_phot_rp_mean_mag',
                  'gaia_phot_rp_mean_flux_over_error', 'gaia_phot_rp_n_obs', 'gaia_phot_variable_flag', 'gaia_astrometric_excess_noise',
                  'gaia_astrometric_excess_noise_sig', 'gaia_astrometric_n_obs_al', 'gaia_astrometric_n_good_obs_al',
                  'gaia_astrometric_weight_al', 'gaia_duplicated_source', 'gaia_a_g_val', 'gaia_e_bp_min_rp_val',
                  'gaia_phot_bp_rp_excess_factor', 'gaia_astrometric_sigma5d_max', 'gaia_astrometric_params_solved',
                  'fiberflux_g', 'fiberflux_r', 'fiberflux_z', 'fibertotflux_g', 'fibertotflux_r', 'fibertotflux_z',
                  'mw_transmission_g', 'mw_transmission_r', 'mw_transmission_z', 'mw_transmission_w1', 'mw_transmission_w2',
                  'mw_transmission_w3', 'mw_transmission_w4', 'nobs_g', 'nobs_r', 'nobs_z', 'nobs_w1', 'nobs_w2',
                  'nobs_w3', 'nobs_w4', 'rchisq_g', 'rchisq_r', 'rchisq_z', 'rchisq_w1', 'rchisq_w2', 'rchisq_w3',
                  'rchisq_w4', 'fracflux_g', 'fracflux_r', 'fracflux_z', 'fracflux_w1', 'fracflux_w2', 'fracflux_w3', 'fracflux_w4',
                  'fracmasked_g', 'fracmasked_r', 'fracmasked_z', 'fracin_g', 'fracin_r', 'fracin_z', 'anymask_g', 'anymask_r', 'anymask_z',
                  'allmask_g', 'allmask_r', 'allmask_z', 'wisemask_w1', 'wisemask_w2', 'psfsize_g', 'psfsize_r', 'psfsize_z', 'psfdepth_g',
                  'psfdepth_r', 'psfdepth_z', 'galdepth_g', 'galdepth_r', 'galdepth_z', 'nea_g', 'nea_r', 'nea_z', 'blob_nea_g',
                  'blob_nea_r', 'blob_nea_z', 'psfdepth_w1', 'psfdepth_w2', 'psfdepth_w3', 'psfdepth_w4', 'wise_coadd_id',
                  'wise_x', 'wise_y', 'sersic', 'sersic_ivar', 'shape_r', 'shape_r_ivar', 'shape_e1', 'shape_e1_ivar',
                  'shape_e2', 'shape_e2_ivar', 'healpix_id_log2nside17', 'flux_g', 'flux_ivar_g', 'flux_r',
                  'flux_ivar_r', 'flux_z', 'flux_ivar_z', 'flux_w1', 'flux_ivar_w1', 'flux_w2', 'flux_ivar_w2', 'flux_w3',
                  'flux_ivar_w3', 'flux_w4', 'flux_ivar_w4', 'counterparts_number', 'single_counterpart', 'counterparts_type',
                  'flux_g_ebv', 'flux_r_ebv', 'flux_z_ebv', 'flux_w1_ebv', 'flux_w2_ebv', 'flux_w3_ebv', 'flux_w4_ebv', 'survey', 'file_name']
        return fields

    @staticmethod
    def get_sdss_fields():
        fields = ['srcname_fin', 'hpidx', 'opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'RAERR', 'DECERR',
                  'cModelFlux_u', 'cModelFluxIvar_u', 'cModelFlux_g', 'cModelFluxIvar_g', 'cModelFlux_r',
                  'cModelFluxIvar_r', 'cModelFlux_i', 'cModelFluxIvar_i', 'cModelFlux_z', 'cModelFluxIvar_z',
                  'psfFlux_u', 'psfFluxIvar_u', 'psfFlux_g', 'psfFluxIvar_g', 'psfFlux_r', 'psfFluxIvar_r',
                  'psfFlux_i', 'psfFluxIvar_i', 'psfFlux_z', 'psfFluxIvar_z', 'counterparts_number',
                  'single_counterpart', 'counterparts_type', 'survey', 'file_name']
        return fields

    @staticmethod
    def get_ps_fields():
        fields = ['srcname_fin', 'hpidx', 'opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'raStack', 'decStack', 'raStackErr', 'decStackErr',
                  'raMean', 'decMean', 'raMeanErr', 'decMeanErr', 'objInfoFlag', 'qualityFlag', 'primaryDetection',
                  'bestDetection', 'duplicat', 'd_to', 'fitext', 'devaucou', 'star', 'w1fit', 'w1bad',
                  'w1mag', 'dw1mag', 'w2fit', 'w2bad', 'w2mag', 'dw2mag', 'gKronFlux', 'gKronFluxErr', 'rKronFlux',
                  'rKronFluxErr', 'iKronFlux', 'iKronFluxErr', 'zKronFlux', 'zKronFluxErr', 'yKronFlux', 'yKronFluxErr',
                  'gPSFFlux', 'gPSFFluxErr', 'rPSFFlux', 'rPSFFluxErr', 'iPSFFlux', 'iPSFFluxErr', 'zPSFFlux',
                  'zPSFFluxErr', 'yPSFFlux', 'yPSFFluxErr', 'w1flux', 'dw1flux', 'w2flux', 'dw2flux',
                  'counterparts_number', 'single_counterpart', 'counterparts_type', 'survey', 'file_name']
        return fields

    @staticmethod
    def get_gaia_fields():
        fields = ['srcname_fin', 'hpidx', 'opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'star', 'ra_error', 'dec_error',
                  'parallax', 'parallax_error', 'pm', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error',
                  'astrometric_n_good_obs_al', 'astrometric_gof_al', 'astrometric_chi2_al', 'astrometric_excess_noise',
                  'astrometric_excess_noise_sig', 'pseudocolour', 'pseudocolour_error', 'visibility_periods_used', 'ruwe',
                  'duplicated_source', 'phot_g_n_obs', 'phot_g_mean_mag', 'phot_bp_mean_flux', 'phot_bp_mean_flux_error',
                  'phot_bp_mean_mag', 'phot_rp_mean_flux', 'phot_rp_mean_flux_error', 'phot_rp_mean_mag',
                  'dr2_radial_velocity', 'dr2_radial_velocity_error', 'l', 'b', 'ecl_lon', 'ecl_lat', 'phot_g_mean_flux',
                  'phot_g_mean_flux_error', 'counterparts_number', 'single_counterpart', 'counterparts_type', 'survey', 'file_name']
        return fields

    @staticmethod
    def get_ls_table_schema():
        fields = [pa.field('srcname_fin', pa.string()),
                  pa.field('hpidx', pa.int64()),
                  pa.field('opt_id', pa.int64()),
                  pa.field('objID', pa.int64()),
                  pa.field('ra', pa.float64(), False),
                  pa.field('dec', pa.float64(), False),
                  pa.field('opt_hpidx', pa.int64()),
                  # flag for AGN wise
                  pa.field('flag_agn_wise', pa.bool_()),
                  # flag for GAIA EDR2 Star
                  pa.field('star', pa.bool_()),

                  pa.field('brick_primary', pa.bool_()),

                  pa.field('maskbits', pa.float64()),
                  pa.field('fitbits', pa.float64()),
                  pa.field('type', pa.string()),
                  pa.field('ra_ivar', pa.float64()),
                  pa.field('dec_ivar', pa.float64()),
                  pa.field('bx', pa.float64()),
                  pa.field('by', pa.float64()),
                  pa.field('ebv', pa.float64()),
                  pa.field('mjd_min', pa.float64()),
                  pa.field('mjd_max', pa.float64()),
                  pa.field('ref_cat', pa.string()),
                  pa.field('ref_id', pa.float64()),

                  pa.field('pmra', pa.float64()),
                  pa.field('pmdec', pa.float64()),
                  pa.field('parallax', pa.float64()),
                  pa.field('pmra_ivar', pa.float64()),
                  pa.field('pmdec_ivar', pa.float64()),
                  pa.field('parallax_ivar', pa.float64()),
                  pa.field('ref_epoch', pa.float64()),

                  pa.field('gaia_phot_g_mean_mag', pa.float64()),
                  pa.field('gaia_phot_g_mean_flux_over_error', pa.float64()),
                  pa.field('gaia_phot_g_n_obs', pa.float64()),
                  pa.field('gaia_phot_bp_mean_mag', pa.float64()),
                  pa.field('gaia_phot_bp_mean_flux_over_error', pa.float64()),
                  pa.field('gaia_phot_bp_n_obs', pa.float64()),
                  pa.field('gaia_phot_rp_mean_mag', pa.float64()),
                  pa.field('gaia_phot_rp_mean_flux_over_error', pa.float64()),
                  pa.field('gaia_phot_rp_n_obs', pa.float64()),

                  pa.field('gaia_phot_variable_flag', pa.bool_()),

                  pa.field('gaia_astrometric_excess_noise', pa.float64()),
                  pa.field('gaia_astrometric_excess_noise_sig', pa.float64()),
                  pa.field('gaia_astrometric_n_obs_al', pa.float64()),
                  pa.field('gaia_astrometric_n_good_obs_al', pa.float64()),
                  pa.field('gaia_astrometric_weight_al', pa.float64()),

                  pa.field('gaia_duplicated_source', pa.bool_()),

                  pa.field('gaia_a_g_val', pa.float64()),
                  pa.field('gaia_e_bp_min_rp_val', pa.float64()),
                  pa.field('gaia_phot_bp_rp_excess_factor', pa.float64()),
                  pa.field('gaia_astrometric_sigma5d_max', pa.float64()),
                  pa.field('gaia_astrometric_params_solved', pa.float64()),

                  pa.field('fiberflux_g', pa.float64()),
                  pa.field('fiberflux_r', pa.float64()),
                  pa.field('fiberflux_z', pa.float64()),
                  pa.field('fibertotflux_g', pa.float64()),
                  pa.field('fibertotflux_r', pa.float64()),
                  pa.field('fibertotflux_z', pa.float64()),

                  pa.field('mw_transmission_g', pa.float64()),
                  pa.field('mw_transmission_r', pa.float64()),
                  pa.field('mw_transmission_z', pa.float64()),
                  pa.field('mw_transmission_w1', pa.float64()),
                  pa.field('mw_transmission_w2', pa.float64()),
                  pa.field('mw_transmission_w3', pa.float64()),
                  pa.field('mw_transmission_w4', pa.float64()),

                  pa.field('nobs_g', pa.int64()),
                  pa.field('nobs_r', pa.int64()),
                  pa.field('nobs_z', pa.int64()),
                  pa.field('nobs_w1', pa.int64()),
                  pa.field('nobs_w2', pa.int64()),
                  pa.field('nobs_w3', pa.int64()),
                  pa.field('nobs_w4', pa.int64()),

                  pa.field('rchisq_g', pa.float64()),
                  pa.field('rchisq_r', pa.float64()),
                  pa.field('rchisq_z', pa.float64()),
                  pa.field('rchisq_w1', pa.float64()),
                  pa.field('rchisq_w2', pa.float64()),
                  pa.field('rchisq_w3', pa.float64()),
                  pa.field('rchisq_w4', pa.float64()),

                  pa.field('fracflux_g', pa.float64()),
                  pa.field('fracflux_r', pa.float64()),
                  pa.field('fracflux_z', pa.float64()),
                  pa.field('fracflux_w1', pa.float64()),
                  pa.field('fracflux_w2', pa.float64()),
                  pa.field('fracflux_w3', pa.float64()),
                  pa.field('fracflux_w4', pa.float64()),

                  pa.field('fracmasked_g', pa.float64()),
                  pa.field('fracmasked_r', pa.float64()),
                  pa.field('fracmasked_z', pa.float64()),
                  pa.field('fracin_g', pa.float64()),
                  pa.field('fracin_r', pa.float64()),
                  pa.field('fracin_z', pa.float64()),

                  pa.field('anymask_g', pa.float64()),
                  pa.field('anymask_r', pa.float64()),
                  pa.field('anymask_z', pa.float64()),
                  pa.field('allmask_g', pa.float64()),
                  pa.field('allmask_r', pa.float64()),
                  pa.field('allmask_z', pa.float64()),

                  pa.field('wisemask_w1', pa.float64()),
                  pa.field('wisemask_w2', pa.float64()),

                  pa.field('psfsize_g', pa.float64()),
                  pa.field('psfsize_r', pa.float64()),
                  pa.field('psfsize_z', pa.float64()),
                  pa.field('psfdepth_g', pa.float64()),
                  pa.field('psfdepth_r', pa.float64()),
                  pa.field('psfdepth_z', pa.float64()),
                  pa.field('galdepth_g', pa.float64()),
                  pa.field('galdepth_r', pa.float64()),
                  pa.field('galdepth_z', pa.float64()),

                  pa.field('nea_g', pa.float64()),
                  pa.field('nea_r', pa.float64()),
                  pa.field('nea_z', pa.float64()),

                  pa.field('blob_nea_g', pa.float64()),
                  pa.field('blob_nea_r', pa.float64()),
                  pa.field('blob_nea_z', pa.float64()),

                  pa.field('psfdepth_w1', pa.float64()),
                  pa.field('psfdepth_w2', pa.float64()),
                  pa.field('psfdepth_w3', pa.float64()),
                  pa.field('psfdepth_w4', pa.float64()),

                  pa.field('wise_coadd_id', pa.string()),

                  pa.field('wise_x', pa.float64()),
                  pa.field('wise_y', pa.float64()),

                  pa.field('sersic', pa.float64()),
                  pa.field('sersic_ivar', pa.float64()),

                  pa.field('shape_r', pa.float64()),
                  pa.field('shape_r_ivar', pa.float64()),
                  pa.field('shape_e1', pa.float64()),
                  pa.field('shape_e1_ivar', pa.float64()),
                  pa.field('shape_e2', pa.float64()),
                  pa.field('shape_e2_ivar', pa.float64()),

                  pa.field('healpix_id_log2nside17', pa.float64()),

                  pa.field('flux_g', pa.float64()),
                  pa.field('flux_ivar_g', pa.float64()),
                  pa.field('flux_r', pa.float64()),
                  pa.field('flux_ivar_r', pa.float64()),
                  pa.field('flux_z', pa.float64()),
                  pa.field('flux_ivar_z', pa.float64()),

                  pa.field('flux_w1', pa.float64()),
                  pa.field('flux_ivar_w1', pa.float64()),
                  pa.field('flux_w2', pa.float64()),
                  pa.field('flux_ivar_w2', pa.float64()),
                  pa.field('flux_w3', pa.float64()),
                  pa.field('flux_ivar_w3', pa.float64()),
                  pa.field('flux_w4', pa.float64()),
                  pa.field('flux_ivar_w4', pa.float64()),

                  pa.field('counterparts_number', pa.float64()),
                  pa.field('single_counterpart', pa.bool_()),
                  pa.field('counterparts_type', pa.string()),

                  pa.field('flux_g_ebv', pa.float64()),
                  pa.field('flux_r_ebv', pa.float64()),
                  pa.field('flux_z_ebv', pa.float64()),
                  pa.field('flux_w1_ebv', pa.float64()),
                  pa.field('flux_w2_ebv', pa.float64()),
                  pa.field('flux_w3_ebv', pa.float64()),
                  pa.field('flux_w4_ebv', pa.float64()),

                  pa.field('survey', pa.int64()),
                  pa.field('file_name', pa.string()),
                  ]
        schema = pa.schema(fields)

        return schema

    @staticmethod
    def get_sdss_table_schema():
        fields = [pa.field('srcname_fin', pa.string()),
                  pa.field('hpidx', pa.int64()),
                  pa.field('opt_id', pa.int64()),
                  pa.field('objID', pa.string()),
                  pa.field('ra', pa.float64(), False),
                  pa.field('dec', pa.float64(), False),
                  pa.field('opt_hpidx', pa.int64()),

                  pa.field('RAERR', pa.float64()),
                  pa.field('DECERR', pa.float64()),

                  pa.field('cModelFlux_u', pa.float64()),
                  pa.field('cModelFluxIvar_u', pa.float64()),
                  pa.field('cModelFlux_g', pa.float64()),
                  pa.field('cModelFluxIvar_g', pa.float64()),
                  pa.field('cModelFlux_r', pa.float64()),
                  pa.field('cModelFluxIvar_r', pa.float64()),
                  pa.field('cModelFlux_i', pa.float64()),
                  pa.field('cModelFluxIvar_i', pa.float64()),
                  pa.field('cModelFlux_z', pa.float64()),
                  pa.field('cModelFluxIvar_z', pa.float64()),

                  pa.field('psfFlux_u', pa.float64()),
                  pa.field('psfFluxIvar_u', pa.float64()),
                  pa.field('psfFlux_g', pa.float64()),
                  pa.field('psfFluxIvar_g', pa.float64()),
                  pa.field('psfFlux_r', pa.float64()),
                  pa.field('psfFluxIvar_r', pa.float64()),
                  pa.field('psfFlux_i', pa.float64()),
                  pa.field('psfFluxIvar_i', pa.float64()),
                  pa.field('psfFlux_z', pa.float64()),
                  pa.field('psfFluxIvar_z', pa.float64()),

                  pa.field('counterparts_number', pa.float64()),
                  pa.field('single_counterpart', pa.bool_()),
                  pa.field('counterparts_type', pa.string()),

                  pa.field('survey', pa.int64()),
                  pa.field('file_name', pa.string()),
                  ]
        schema = pa.schema(fields)

        return schema

    @staticmethod
    def get_ps_table_schema():
        fields = [pa.field('srcname_fin', pa.string()),
                  pa.field('hpidx', pa.int64()),
                  pa.field('opt_id', pa.int64()),
                  pa.field('objID', pa.string()),
                  pa.field('ra', pa.float64(), False),
                  pa.field('dec', pa.float64(), False),
                  pa.field('opt_hpidx', pa.int64()),
                  
                  pa.field('raStack', pa.float64()),
                  pa.field('decStack', pa.float64()),
                  pa.field('raStackErr', pa.float64()),
                  pa.field('decStackErr', pa.float64()),
                  pa.field('raMean', pa.float64()),
                  pa.field('decMean', pa.float64()),
                  pa.field('raMeanErr', pa.float64()),
                  pa.field('decMeanErr', pa.float64()),

                  pa.field('objInfoFlag', pa.float64()),
                  pa.field('qualityFlag', pa.int64()),

                  pa.field('primaryDetection', pa.int64()),
                  pa.field('bestDetection', pa.int64()),

                  pa.field('duplicat', pa.bool_()),
                  pa.field('d_to', pa.int64()),
                  pa.field('fitext', pa.bool_()),
                  pa.field('devaucou', pa.bool_()),
                  pa.field('star', pa.bool_()),

                  pa.field('w1fit', pa.bool_()),
                  pa.field('w1bad', pa.bool_()),
                  pa.field('w1mag', pa.float64()),
                  pa.field('dw1mag', pa.float64()),

                  pa.field('w2fit', pa.bool_()),
                  pa.field('w2bad', pa.bool_()),
                  pa.field('w2mag', pa.float64()),
                  pa.field('dw2mag', pa.float64()),

                  pa.field('gKronFlux', pa.float64()),
                  pa.field('gKronFluxErr', pa.float64()),
                  pa.field('rKronFlux', pa.float64()),
                  pa.field('rKronFluxErr', pa.float64()),
                  pa.field('iKronFlux', pa.float64()),
                  pa.field('iKronFluxErr', pa.float64()),
                  pa.field('zKronFlux', pa.float64()),
                  pa.field('zKronFluxErr', pa.float64()),
                  pa.field('yKronFlux', pa.float64()),
                  pa.field('yKronFluxErr', pa.float64()),

                  pa.field('gPSFFlux', pa.float64()),
                  pa.field('gPSFFluxErr', pa.float64()),
                  pa.field('rPSFFlux', pa.float64()),
                  pa.field('rPSFFluxErr', pa.float64()),
                  pa.field('iPSFFlux', pa.float64()),
                  pa.field('iPSFFluxErr', pa.float64()),
                  pa.field('zPSFFlux', pa.float64()),
                  pa.field('zPSFFluxErr', pa.float64()),
                  pa.field('yPSFFlux', pa.float64()),
                  pa.field('yPSFFluxErr', pa.float64()),

                  pa.field('w1flux', pa.float64()),
                  pa.field('dw1flux', pa.float64()),
                  pa.field('w2flux', pa.float64()),
                  pa.field('dw2flux', pa.float64()),

                  pa.field('counterparts_number', pa.float64()),
                  pa.field('single_counterpart', pa.bool_()),
                  pa.field('counterparts_type', pa.string()),

                  pa.field('survey', pa.int64()),
                  pa.field('file_name', pa.string()),
                  ]
        schema = pa.schema(fields)

        return schema

    @staticmethod
    def get_gaia_table_schema():
        fields = [pa.field('srcname_fin', pa.string()),
                  pa.field('hpidx', pa.int64()),
                  pa.field('opt_id', pa.int64()),
                  pa.field('objID', pa.string()),
                  pa.field('ra', pa.float64(), False),
                  pa.field('dec', pa.float64(), False),
                  pa.field('opt_hpidx', pa.int64()),
                  pa.field('star', pa.bool_()),
                  
                  pa.field('ra_error', pa.float64()),
                  pa.field('dec_error', pa.float64()),
                  pa.field('parallax', pa.float64()),
                  pa.field('parallax_error', pa.float64()),

                  pa.field('pm', pa.float64()),
                  pa.field('pmra', pa.float64()),
                  pa.field('pmra_error', pa.float64()),
                  pa.field('pmdec', pa.float64()),
                  pa.field('pmdec_error', pa.float64()),

                  pa.field('astrometric_n_good_obs_al', pa.int64()),
                  pa.field('astrometric_gof_al', pa.float64()),
                  pa.field('astrometric_chi2_al', pa.float64()),
                  pa.field('astrometric_excess_noise', pa.float64()),
                  pa.field('astrometric_excess_noise_sig', pa.float64()),

                  pa.field('pseudocolour', pa.float64()),
                  pa.field('pseudocolour_error', pa.float64()),

                  pa.field('visibility_periods_used', pa.int64()),
                  pa.field('ruwe', pa.float64()),
                  pa.field('duplicated_source', pa.bool_()),

                  pa.field('phot_g_n_obs', pa.int64()),
                  pa.field('phot_g_mean_mag', pa.float64()),
                  pa.field('phot_bp_mean_flux', pa.float64()),
                  pa.field('phot_bp_mean_flux_error', pa.float64()),
                  pa.field('phot_bp_mean_mag', pa.float64()),
                  pa.field('phot_rp_mean_flux', pa.float64()),
                  pa.field('phot_rp_mean_flux_error', pa.float64()),
                  pa.field('phot_rp_mean_mag', pa.float64()),

                  pa.field('dr2_radial_velocity', pa.float64()),
                  pa.field('dr2_radial_velocity_error', pa.float64()),

                  pa.field('l', pa.float64()),
                  pa.field('b', pa.float64()),

                  pa.field('ecl_lon', pa.float64()),
                  pa.field('ecl_lat', pa.float64()),

                  pa.field('phot_g_mean_flux', pa.float64()),
                  pa.field('phot_g_mean_flux_error', pa.float64()),

                  pa.field('counterparts_number', pa.float64()),
                  pa.field('single_counterpart', pa.bool_()),
                  pa.field('counterparts_type', pa.string()),

                  pa.field('survey', pa.int64()),
                  pa.field('file_name', pa.string()),
                  ]
        schema = pa.schema(fields)

        return schema

    @staticmethod
    def get_opt_survey_sources(self, opt_sources, opt_fields, opt_type='ls_'):
        # take sources from optical survey
        obj_id = opt_type + 'objID'
        survey_sources = opt_sources[opt_sources[obj_id].notnull()]  # drop NaN opt sources
        # get fields with prefix opt_type
        fields = [(opt_type + field) if field not in self.base_fields else field for field in opt_fields]
        # Check if all field names in dataframe - add them
        for field in fields:
            if field not in survey_sources.columns:
                survey_sources[field] = None

        survey_sources = survey_sources[fields]
        # drop duplicates
        survey_sources.drop_duplicates(subset=[(opt_type+'ra'), (opt_type+'dec')], inplace=True)
        # rename columns - cut prefix 'ls_'
        survey_sources.columns = opt_fields

        print(f'\nTable with {opt_type}sources:\n', survey_sources)

        return survey_sources

    @staticmethod
    def get_ls_ps_file_paths(dir_name, survey_num):
        # get file with ls data and ps data by survey number
        # TODO: change file path later
        if survey_num == 1:
            ls_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS1_ls_50.pkl')
            ps_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS1_ps_50.pkl')
        elif survey_num == 2:
            ls_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS2_ls_50.pkl')
            ps_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS2_ps_50.pkl')
        elif survey_num == 3:
            ls_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS3_ls_50.pkl')
            ps_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS3_ps_50.pkl')
        elif survey_num == 4:
            ls_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS4_ls_50.pkl')
            ps_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS4_ps_50.pkl')
        elif survey_num == 9:
            ls_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS1234_ls_50.pkl')
            ps_file_path = os.path.join(settings.OPTICAL_DIR, 'eRASS1234_ps_50.pkl')

        return ls_file_path, ps_file_path

    def handle(self, *args, **options):
        start_time = timezone.now()
        survey_num = options['survey_num']
        # get dir name by survey number
        dir_name = 'eRASS' + str(survey_num)
        # get file paths by survey number
        ls_file_path, ps_file_path = Command.get_ls_ps_file_paths(dir_name, survey_num)
        # create dirs for optic data
        # check path
        print(os.path.join(settings.WORK_DIR, dir_name))
        if not os.path.exists(os.path.join(settings.WORK_DIR, dir_name)):
            os.makedirs(os.path.join(settings.WORK_DIR, dir_name))

        # load opt sources correlated with DESI LIS
        with open(ls_file_path, 'rb') as f:
            opt_sources_ls = pickle.load(f)

        # load opt sources correlated with Pan-STARRs
        with open(ps_file_path, 'rb') as f:
            opt_sources_ps = pickle.load(f)

        print(f'Table with opt_sources_ls:\n', opt_sources_ls)
        print(f'Table with opt_sources_ps:\n', opt_sources_ps)

        # get names of xray sources without correlated ls optical sources
        null_ls_sources = list(opt_sources_ls[opt_sources_ls['ls_objid'].isnull()]['srcname_fin'])
        # delete NULL rows
        opt_sources_ls = opt_sources_ls[opt_sources_ls['ls_objid'].notnull()]
        print(f'Table with notnull opt_sources_ls:\n', opt_sources_ls)

        # get ps correlated optical sources for xray sources found earlier
        opt_sources_ps = opt_sources_ps.query('srcname_fin in @null_ls_sources')
        # shuffle and limit number of ps opt sources
        # opt_sources_ps = opt_sources_ps.sample(frac=1)
        # opt_sources_ps = opt_sources_ps.groupby('srcname_fin').head(20)
        print(f'Table with filtered opt_sources_ps:\n', opt_sources_ps)

        # Concatenate sources correlated with ls and correlated with ps
        opt_sources = pd.concat([opt_sources_ps, opt_sources_ls])
        print(f'Table with concatenated ps sources and ls sources:\n', opt_sources)

        # rename columns of opt sources
        opt_sources = opt_sources.rename(columns={'ls_objid': 'ls_objID', 'ps_raBest': 'ps_ra', 'ps_decBest': 'ps_dec',
                                                  'gaiaedr3_designation': 'gaiaedr3_objID'})

        # index sources in each group(same xray source) + const for complicated cases with opt sources from dif surveys
        opt_sources['opt_id'] = opt_sources.groupby('srcname_fin').cumcount() + (survey_num-1)*100
        # add file name
        file_name = os.path.splitext(os.path.basename(ls_file_path))[0]
        opt_sources['file_name'] = file_name

        # TODO: change this later (datetime64[ns] -> string)
        for col in opt_sources.columns:
            if is_datetime(opt_sources[col]):
                print("Column {} type: {}".format(col, opt_sources[col].dtype))
                opt_sources[col] = pd.to_datetime(opt_sources[col]).dt.date
                opt_sources[col] = opt_sources[col].astype(str)

        opt_sources['survey'] = survey_num

        # # get DESI LIS sources
        ls_fields = Command.get_ls_fields()
        ls_sources = Command.get_opt_survey_sources(self, opt_sources, ls_fields, opt_type='ls_')
        # calculate AGN WISE flag
        ls_mag_w1 = 22.5 - 2.5 * np.log10(ls_sources['flux_w1'])  # if w1 < 0 -> None, flag=False
        ls_mag_w2 = 22.5 - 2.5 * np.log10(ls_sources['flux_w2'])  # if w2 < 0 -> None, flag=False
        ls_sources['flag_agn_wise'] = ((ls_mag_w1 - ls_mag_w2) > 0.8)
        # calculate star flag GAIA EDR2
        ls_sources['star'] = ((abs(ls_sources['parallax'] * np.sqrt(ls_sources['parallax_ivar'])) > 5)
                              | (abs(ls_sources['pmra'] * np.sqrt(ls_sources['pmra_ivar'])) > 5)
                              | (abs(ls_sources['pmdec'] * np.sqrt(ls_sources['pmdec_ivar'])) > 5))

        # Save parquet table with specified schema
        ls_schema = Command.get_ls_table_schema()
        table = pa.Table.from_pandas(ls_sources, schema=ls_schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_ls.parquet'))

        # # get SDSS source
        sdss_fields = Command.get_sdss_fields()
        sdss_sources = Command.get_opt_survey_sources(self, opt_sources, sdss_fields, opt_type='sdss_')
        # Save parquet table with specified schema
        sdss_schema = Command.get_sdss_table_schema()
        table = pa.Table.from_pandas(sdss_sources, schema=sdss_schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_sdss.parquet'))

        # # get PS source
        ps_fields = Command.get_ps_fields()
        ps_sources = Command.get_opt_survey_sources(self, opt_sources, ps_fields, opt_type='ps_')
        # Save parquet table with specified schema
        ps_schema = Command.get_ps_table_schema()
        table = pa.Table.from_pandas(ps_sources, schema=ps_schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_ps.parquet'))

        # # get GAIA source
        gaia_fields = Command.get_gaia_fields()
        gaia_sources = Command.get_opt_survey_sources(self, opt_sources, gaia_fields, opt_type='gaiaedr3_')
        # convert string column to boolean
        gaia_sources['duplicated_source'].replace({'True ': True, 'True': True,
                                                   'False': False, 'False ': False}, inplace=True)
        # calculate star flag GAIA EDR3
        gaia_sources['star'] = ((abs(gaia_sources['parallax'] / gaia_sources['parallax_error']) > 5)
                                | (abs(gaia_sources['pmra'] / gaia_sources['pmra_error']) > 5)
                                | (abs(gaia_sources['pmdec'] / gaia_sources['pmdec_error']) > 5))

        # Save parquet table with specified schema
        gaia_schema = Command.get_gaia_table_schema()
        table = pa.Table.from_pandas(gaia_sources, schema=gaia_schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_gaia.parquet'))

        self.stdout.write(f'End converting pkl')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Converting PKL took: {(end_time - start_time).total_seconds()} seconds.'))
