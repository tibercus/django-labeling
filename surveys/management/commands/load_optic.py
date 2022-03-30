from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from surveys.models import *
from django.conf import settings
import shutil
import os

import astropy.units as u
from astropy.coordinates import SkyCoord

import astropy_healpix
from astropy_healpix import HEALPix
from decimal import Decimal


class Command(BaseCommand):
    help = "Load Optical data from Parquet file."

    def add_arguments(self, parser):
        parser.add_argument('survey_num', type=int, help='number of survey')

    @staticmethod
    def get_ls_fields():
        fields = ['opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'brick_primary', 'maskbits', 'fitbits', 'type', 'ra_ivar',
                  'dec_ivar', 'bx', 'by', 'ebv', 'mjd_min', 'mjd_max', 'ref_cat', 'ref_id', 'pmra', 'pmdec', 'parallax',
                  'pmra_ivar', 'pmdec_ivar', 'parallax_ivar', 'ref_epoch', 'gaia_phot_g_mean_mag', 'gaia_phot_g_mean_flux_over_error',
                  'gaia_phot_g_n_obs', 'gaia_phot_bp_mean_mag', 'gaia_phot_bp_mean_flux_over_error', 'gaia_phot_bp_n_obs',
                  'gaia_phot_rp_mean_mag', 'gaia_phot_rp_mean_flux_over_error', 'gaia_phot_rp_n_obs', 'gaia_phot_variable_flag',
                  'gaia_astrometric_excess_noise', 'gaia_astrometric_excess_noise_sig', 'gaia_astrometric_n_obs_al',
                  'gaia_astrometric_n_good_obs_al', 'gaia_astrometric_weight_al', 'gaia_duplicated_source', 'gaia_a_g_val',
                  'gaia_e_bp_min_rp_val', 'gaia_phot_bp_rp_excess_factor', 'gaia_astrometric_sigma5d_max', 'gaia_astrometric_params_solved',
                  'fiberflux_g', 'fiberflux_r', 'fiberflux_z', 'fibertotflux_g', 'fibertotflux_r', 'fibertotflux_z',
                  'mw_transmission_g', 'mw_transmission_r', 'mw_transmission_z', 'mw_transmission_w1',
                  'mw_transmission_w2', 'mw_transmission_w3', 'mw_transmission_w4', 'nobs_g', 'nobs_r', 'nobs_z', 'nobs_w1', 'nobs_w2',
                  'nobs_w3', 'nobs_w4', 'rchisq_g', 'rchisq_r', 'rchisq_z', 'rchisq_w1', 'rchisq_w2', 'rchisq_w3',
                  'rchisq_w4', 'fracflux_g', 'fracflux_r', 'fracflux_z', 'fracflux_w1', 'fracflux_w2', 'fracflux_w3',
                  'fracflux_w4', 'fracmasked_g', 'fracmasked_r', 'fracmasked_z', 'fracin_g', 'fracin_r', 'fracin_z', 'anymask_g',
                  'anymask_r', 'anymask_z',
                  'allmask_g', 'allmask_r', 'allmask_z', 'wisemask_w1', 'wisemask_w2', 'psfsize_g', 'psfsize_r',
                  'psfsize_z', 'psfdepth_g',
                  'psfdepth_r', 'psfdepth_z', 'galdepth_g', 'galdepth_r', 'galdepth_z', 'nea_g', 'nea_r', 'nea_z',
                  'blob_nea_g', 'blob_nea_r', 'blob_nea_z', 'psfdepth_w1', 'psfdepth_w2', 'psfdepth_w3', 'psfdepth_w4',
                  'wise_coadd_id', 'wise_x', 'wise_y', 'sersic', 'sersic_ivar', 'shape_r', 'shape_r_ivar', 'shape_e1', 'shape_e1_ivar',
                  'shape_e2', 'shape_e2_ivar', 'healpix_id_log2nside17', 'flux_g', 'flux_ivar_g', 'flux_r',
                  'flux_ivar_r', 'flux_z', 'flux_ivar_z', 'flux_w1', 'flux_ivar_w1', 'flux_w2', 'flux_ivar_w2',
                  'flux_w3', 'flux_ivar_w3', 'flux_w4', 'flux_ivar_w4', 'counterparts_number', 'single_counterpart',
                  'counterparts_type',
                  'flux_g_ebv', 'flux_r_ebv', 'flux_z_ebv', 'flux_w1_ebv', 'flux_w2_ebv', 'flux_w3_ebv', 'flux_w4_ebv',
                  'survey', 'file_name']

        return fields

    @staticmethod
    def get_sdss_fields():
        fields = ['opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'RAERR', 'DECERR',
                  'cModelFlux_u', 'cModelFluxIvar_u', 'cModelFlux_g', 'cModelFluxIvar_g', 'cModelFlux_r',
                  'cModelFluxIvar_r', 'cModelFlux_i', 'cModelFluxIvar_i', 'cModelFlux_z', 'cModelFluxIvar_z',
                  'psfFlux_u', 'psfFluxIvar_u', 'psfFlux_g', 'psfFluxIvar_g', 'psfFlux_r', 'psfFluxIvar_r',
                  'psfFlux_i', 'psfFluxIvar_i', 'psfFlux_z', 'psfFluxIvar_z', 'counterparts_number',
                  'single_counterpart', 'counterparts_type', 'survey', 'file_name']
        return fields

    @staticmethod
    def get_ps_fields():
        fields = ['opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'raStack', 'decStack', 'raStackErr', 'decStackErr',
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
        fields = ['opt_id', 'objID', 'ra', 'dec', 'opt_hpidx', 'ra_error', 'dec_error',
                  'parallax', 'parallax_error', 'pm', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error',
                  'astrometric_n_good_obs_al', 'astrometric_gof_al', 'astrometric_chi2_al', 'astrometric_excess_noise',
                  'astrometric_excess_noise_sig', 'pseudocolour', 'pseudocolour_error', 'visibility_periods_used',
                  'ruwe', 'duplicated_source', 'phot_g_n_obs', 'phot_g_mean_mag', 'phot_bp_mean_flux',
                  'phot_bp_mean_flux_error', 'phot_bp_mean_mag', 'phot_rp_mean_flux', 'phot_rp_mean_flux_error',
                  'phot_rp_mean_mag', 'dr2_radial_velocity', 'dr2_radial_velocity_error',
                  'l', 'b', 'ecl_lon', 'ecl_lat', 'phot_g_mean_flux', 'phot_g_mean_flux_error',
                  'counterparts_number', 'single_counterpart', 'counterparts_type', 'survey', 'file_name']
        return fields

    @staticmethod
    def find_dup_source(xray_sources, opt_source, c_opt, opt_type):
        # for comparing float values
        TOLERANCE = 10 ** -6
        # c_opt = SkyCoord(ra=opt_source.ra * u.deg, dec=opt_source.dec * u.deg, frame='icrs')
        for xray_source in xray_sources:
            c_xray = SkyCoord(ra=xray_source.RA * u.deg, dec=xray_source.DEC * u.deg, distance=1 * u.pc, frame='icrs')
            sep = c_xray.separation(c_opt).arcsecond
            # find/create new opt LS counterpart + get new separation
            if opt_type == 'LS' and (not xray_source.ls_dup or xray_source.ls_dup_sep > sep):
                print(f'Old Sep: {xray_source.ls_dup_sep} New Sep: {sep}')
                xray_source.ls_dup = opt_source
                xray_source.ls_dup_sep = sep
            # find/create new opt SDSS counterpart
            elif opt_type == 'SDSS' and (not xray_source.sdss_dup or xray_source.sdss_dup_sep > sep):
                print(f'Old Sep: {xray_source.sdss_dup_sep} New Sep: {sep}')
                xray_source.sdss_dup = opt_source
                xray_source.sdss_dup_sep = sep
            # find/create new opt PS counterpart
            elif opt_type == 'PS' and (not xray_source.ps_dup or xray_source.ps_dup_sep > sep):
                print(f'Old Sep: {xray_source.ps_dup_sep} New Sep: {sep}')
                xray_source.ps_dup = opt_source
                xray_source.ps_dup_sep = sep
            # find/create new opt PS counterpart
            elif opt_type == 'GAIA' and (not xray_source.gaia_dup or xray_source.gaia_dup_sep > sep):
                print(f'Old Sep: {xray_source.gaia_dup_sep} New Sep: {sep}')
                xray_source.gaia_dup = opt_source
                xray_source.gaia_dup_sep = sep

            xray_source.save()

    def load_opt_survey(self, data, field_list, hp, opt_type='LS'):
        self.stdout.write(f'Start loading {opt_type} optical data')
        for row in data.itertuples():
            # find/create meta source - file
            origin_file, f_created = OriginFile.objects.get_or_create(file_name=row.file_name)
            if f_created:
                self.stdout.write(f'Create new file object for: {row.file_name}')

            # get heal pix index
            # distance = 1*u.pc for futher calculation of cartesian coords
            c_opt = SkyCoord(ra=row.ra * u.deg, dec=row.dec * u.deg, distance=1 * u.pc, frame='icrs')
            opt_hpidx = hp.skycoord_to_healpix(c_opt)

            # find/create optical source
            if opt_type == 'LS':
                opt_source, created = LS.objects.get_or_create(opt_hpidx=opt_hpidx, origin_file=origin_file,
                                                               defaults={'objID': row.objID, 'ra': row.ra, 'dec': row.dec})
            elif opt_type == 'SDSS':
                opt_source, created = SDSS.objects.get_or_create(opt_hpidx=opt_hpidx, origin_file=origin_file,
                                                                 defaults={'objID': row.objID, 'ra': row.ra, 'dec': row.dec})
            elif opt_type == 'PS':
                opt_source, created = PS.objects.get_or_create(opt_hpidx=opt_hpidx, origin_file=origin_file,
                                                               defaults={'objID': row.objID, 'ra': row.ra, 'dec': row.dec})
            elif opt_type == 'GAIA':
                opt_source, created = GAIA.objects.get_or_create(opt_hpidx=opt_hpidx, origin_file=origin_file,
                                                                 defaults={'objID': row.objID, 'ra': row.ra, 'dec': row.dec})

            if created:
                self.stdout.write(f'{row[0]} - Create new {opt_type} source {opt_hpidx} with objID: {row.objID}')

            # Check that it is new source or new file
            if f_created or created:
                try:
                    self.stdout.write(f'Start filling {opt_type} fields...\n')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+3]}')  # i+3 skip index and xray fields
                        filled_fields = ['objID', 'opt_hpidx', 'survey', 'file_name', 'ra', 'dec']
                        if field not in filled_fields:
                            setattr(opt_source, field, row[i+3])  # Similar to source.field = row[i+3]

                    # calculate cartesian coordinates with sphere radius = 1*u.pc
                    opt_source.c_x = c_opt.cartesian.x.value
                    opt_source.c_y = c_opt.cartesian.y.value
                    opt_source.c_z = c_opt.cartesian.z.value
                    # sources.append(source)
                    opt_source.save()
                    # find xray sources for ls source
                    xray_sources = eROSITA.objects.filter(name=row.srcname_fin, survey__name=row.survey,
                                                          hpidx=row.hpidx)
                    if xray_sources.exists():
                        opt_source.xray_sources.add(*xray_sources)
                        # find counter part for xray sources
                        Command.find_dup_source(xray_sources, opt_source, c_opt, opt_type)
                        opt_source.save()
                        print(f'Link {opt_type} source: {opt_source} with xray sources: {xray_sources}\n')
                    else:
                        raise CommandError(f'Cant find xray sources with name: {row.srcname_fin} hpidx: {row.hpidx}'
                                           f'from survey {row.survey}')

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: opt_source.delete()
                    raise CommandError(e)

    def handle(self, *args, **options):
        start_time = timezone.now()
        survey_num = options['survey_num']
        # get dir name by survey number
        dir_name = 'eRASS' + str(survey_num)

        # heal pix map with pixel_resolution < 1/2 arcsec
        hp = HEALPix(nside=2 ** 19, order='nested', frame='icrs')

        self.stdout.write(f'Start reading optical data')
        # sources = []

        # Load DESI LIS sources
        file_path = os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_ls.parquet')

        table = pq.read_table(file_path)
        ls_data = table.to_pandas()

        # replace all nan with None
        ls_data = ls_data.replace({np.nan: None})
        print(f'\nTable with LS sources:\n', ls_data)

        ls_field_list = Command.get_ls_fields()
        Command.load_opt_survey(self, ls_data, ls_field_list, hp, opt_type='LS')

        # Load SDSS sources
        file_path = os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_sdss.parquet')

        table = pq.read_table(file_path)
        sdss_data = table.to_pandas()

        # replace all nan with None
        sdss_data = sdss_data.replace({np.nan: None})
        print(f'\nTable with SDSS sources:\n', sdss_data)

        sdss_field_list = Command.get_sdss_fields()
        Command.load_opt_survey(self, sdss_data, sdss_field_list, hp, opt_type='SDSS')

        # Load PS sources
        file_path = os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_ps.parquet')

        table = pq.read_table(file_path)
        ps_data = table.to_pandas()

        # replace all nan with None
        ps_data = ps_data.replace({np.nan: None})
        print(f'\nTable with PS sources:\n', ps_data)

        ps_field_list = Command.get_ps_fields()
        Command.load_opt_survey(self, ps_data, ps_field_list, hp, opt_type='PS')

        # Load GAIA sources
        file_path = os.path.join(settings.WORK_DIR, dir_name, 'opt_sources_gaia.parquet')

        table = pq.read_table(file_path)
        gaia_data = table.to_pandas()

        # replace all nan with None
        gaia_data = gaia_data.replace({np.nan: None})
        print(f'\nTable with GAIA sources:\n', gaia_data)

        gaia_field_list = Command.get_gaia_fields()
        Command.load_opt_survey(self, gaia_data, gaia_field_list, hp, opt_type='GAIA')

        # maybe use this later
        # if len(sources) > 500:
        #     Source.objects.bulk_create(sources)
        #     sources = []
        #

        # if sources:
        #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading table')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading Parquet took: {(end_time-start_time).total_seconds()} seconds.'))
