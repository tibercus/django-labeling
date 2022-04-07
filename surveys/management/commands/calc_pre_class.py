from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd
import numpy as np

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
    help = "Calculate pre-class for loaded eROSITA Sources."

    @staticmethod
    def calculate_wise_agn(xray_source, c_xray, Rc):
        # flag_agn_wise = 1 - w1 - w2 > 0.8 для ВСЕХ источников в кружке Rc
        # flag_agn_wise = 0 - все остальные случаи
        ls_sources = xray_source.ls_sources.all()
        # TODO: what to return - False or None?
        if not ls_sources.exists():
            return None
        else:
            # get ra, dec of ls sources
            ra = ls_sources.values_list('ra', flat=True)
            dec = ls_sources.values_list('dec', flat=True)
            # get sky coords for ls sources
            c_opts = SkyCoord(ra=ra * u.degree, dec=dec * u.degree, distance=1 * u.pc, frame='icrs')
            # get separations in arcseconds
            sep_arcsec = np.array(c_xray.separation(c_opts).arcsecond)
            # get indices of near sources
            ls_indices = np.argwhere(sep_arcsec < Rc).flatten()
            print(ls_indices)
            # get near ls sources
            for ls_ind in ls_indices:
                near_ls = ls_sources[int(ls_ind)]
                print(f'{ls_ind}: LS Source in radius of correlation: {near_ls} flag_agn_wise: {near_ls.flag_agn_wise}')
                # return False if any source has False flag
                if not near_ls.flag_agn_wise:
                    return False
            # return True if all sources are AGN in radius of correlation
            return True

    @staticmethod
    def calculate_gaia_star(xray_source, c_xray, Rc):
        # g_s = 1 - все источники Гаяй внутри Rc звезды
        # g_s = 0 - ... не звезды
        # g_s = 2	- звезды и не звезды
        # g_s = -1 - нет источников Гайя внутри Rc
        gaia_sources = xray_source.gaia_sources.all()
        # TODO: what to return - -1 or None?
        if not gaia_sources.exists():
            return None
        else:
            # get ra, dec of gaia sources
            ra = gaia_sources.values_list('ra', flat=True)
            dec = gaia_sources.values_list('dec', flat=True)
            # get sky coords for gaia sources
            c_opts = SkyCoord(ra=ra * u.degree, dec=dec * u.degree, distance=1 * u.pc, frame='icrs')
            # get separations in arcseconds
            sep_arcsec = np.array(c_xray.separation(c_opts).arcsecond)
            # get indices of near sources
            gaia_indices = np.argwhere(sep_arcsec < Rc).flatten()
            print(gaia_indices)
            # return -1 if there is no sources in Rc
            if len(gaia_indices) == 0:
                return -1
            # there is opt sources in Rc
            g_s = -1
            # get near gaia sources
            for gaia_ind in gaia_indices:
                near_gaia = gaia_sources[int(gaia_ind)]
                print(f'{gaia_ind}: GAIA Source in radius of correlation: {near_gaia} star: {near_gaia.star}')
                if not near_gaia.star and g_s == -1:
                    g_s = 0
                elif near_gaia.star and g_s == -1:
                    g_s = 1
                elif (not near_gaia.star and g_s == 1) or (near_gaia.star and g_s == 0):
                    return 2
            # return 0 - no stars in Rc, 1 - all stars in Rc, 2 - stars and no stars
            return g_s

    @staticmethod
    def calculate_ls_star(xray_source, c_xray, Rc):
        # g_s = 1 - все источники DESI LS внутри Rc звезды
        # g_s = 0 - ... не звезды
        # g_s = 2	- звезды и не звезды
        # g_s = -1 - нет источников DESI LS внутри Rc
        ls_sources = xray_source.ls_sources.all()
        # TODO: what to return - -1 or None?
        if not ls_sources.exists():
            return None
        else:
            # get ra, dec of gaia sources
            ra = ls_sources.values_list('ra', flat=True)
            dec = ls_sources.values_list('dec', flat=True)
            # get sky coords for gaia sources
            c_opts = SkyCoord(ra=ra * u.degree, dec=dec * u.degree, distance=1 * u.pc, frame='icrs')
            # get separations in arcseconds
            sep_arcsec = np.array(c_xray.separation(c_opts).arcsecond)
            # get indices of near sources
            ls_indices = np.argwhere(sep_arcsec < Rc).flatten()
            print(ls_indices)
            # return -1 if there is no sources in Rc
            if len(ls_indices) == 0:
                return -1
            # there is opt sources in Rc
            ls_g_s = -1
            # get near gaia sources
            for ls_ind in ls_indices:
                near_ls = ls_sources[int(ls_ind)]
                print(f'{ls_ind}: LS Source in radius of correlation: {near_ls} star: {near_ls.star}')
                if not near_ls.star and ls_g_s == -1:
                    ls_g_s = 0
                elif near_ls.star and ls_g_s == -1:
                    ls_g_s = 1
                elif (not near_ls.star and ls_g_s == 1) or (near_ls.star and ls_g_s == 0):
                    return 2
            # return 0 - no stars in Rc, 1 - all stars in Rc, 2 - stars and no stars
            return ls_g_s

    @staticmethod
    def calculate_tde_v3(meta_object):
        # ID_e3 == -1 & g_s != 1 &  (qual != 0 & qual != 2) & flag_agn_wise != 1 & RATIO_e4e3 > 7
        survey_2_flag = meta_object.ID_e1 == -1 and (meta_object.g_s is not None) and meta_object.g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e2e1 is not None) and meta_object.RATIO_e2e1 > 7

        survey_3_flag = meta_object.ID_e2 == -1 and (meta_object.g_s is not None) and meta_object.g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e3e2 is not None) and meta_object.RATIO_e3e2 > 7

        survey_4_flag = meta_object.ID_e3 == -1 and (meta_object.g_s is not None) and meta_object.g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e4e3 is not None) and meta_object.RATIO_e4e3 > 7

        # survey_5_flag = meta_object.ID_e4 == -1 and meta_object.g_s and meta_object.g_s != 1 \
        #                         and meta_object.flag_agn_wise and meta_object.flag_agn_wise != 1 \
        #                         and meta_object.RATIO_e5e4 and meta_object.RATIO_e5e4 > 7

        tde_v3 = survey_2_flag or survey_3_flag or survey_4_flag
        print(f'Surveys flags: {survey_2_flag}, {survey_3_flag}, {survey_4_flag} = {tde_v3}\n')
        return tde_v3

    @staticmethod
    def calculate_tde_v3_ls(meta_object):
        # ID_e3 == -1 & ls_g_s != 1 &  (qual != 0 & qual != 2) & flag_agn_wise != 1 & RATIO_e4e3 > 7
        survey_2_flag = meta_object.ID_e1 == -1 and (meta_object.ls_g_s is not None) and meta_object.ls_g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e2e1 is not None) and meta_object.RATIO_e2e1 > 7

        survey_3_flag = meta_object.ID_e2 == -1 and (meta_object.ls_g_s is not None) and meta_object.ls_g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e3e2 is not None) and meta_object.RATIO_e3e2 > 7

        survey_4_flag = meta_object.ID_e3 == -1 and (meta_object.ls_g_s is not None) and meta_object.ls_g_s != 1 \
                        and (meta_object.flag_agn_wise is not None) and meta_object.flag_agn_wise != 1 \
                        and (meta_object.RATIO_e4e3 is not None) and meta_object.RATIO_e4e3 > 7

        # survey_5_flag = meta_object.ID_e4 == -1 and meta_object.ls_g_s and meta_object.ls_g_s != 1 \
        #                         and meta_object.flag_agn_wise and meta_object.flag_agn_wise != 1 \
        #                         and meta_object.RATIO_e5e4 and meta_object.RATIO_e5e4 > 7

        tde_v3_ls = survey_2_flag or survey_3_flag or survey_4_flag
        print(f'Surveys flags: {survey_2_flag}, {survey_3_flag}, {survey_4_flag} = {tde_v3_ls}\n')
        return tde_v3_ls

    def handle(self, *args, **options):
        start_time = timezone.now()
        for xray_source in eROSITA.objects.all():
            # calculate radius of correlation
            Rc = np.clip(1.1 * xray_source.pos_r98, 4, 20)
            # get SkyCoord for calculating separation
            c_xray = SkyCoord(ra=xray_source.RA * u.degree, dec=xray_source.DEC * u.degree, distance=1 * u.pc,
                              frame='icrs')
            print(f'Radius of correlation:{Rc}')
            # get flag values
            if xray_source.flag_agn_wise is None:
                xray_source.flag_agn_wise = Command.calculate_wise_agn(xray_source, c_xray, Rc)
                print(f'AGN Flag for {xray_source.survey.name} - {xray_source}: {xray_source.flag_agn_wise}\n')

            if xray_source.g_s is None:
                xray_source.g_s = Command.calculate_gaia_star(xray_source, c_xray, Rc)
                print(f'GAIA EDR3 Star Flag for {xray_source.survey.name} - {xray_source}: {xray_source.g_s}\n')

            if xray_source.ls_g_s is None:
                xray_source.ls_g_s = Command.calculate_ls_star(xray_source, c_xray, Rc)
                print(f'GAIA EDR2 Star Flag for {xray_source.survey.name} - {xray_source}: {xray_source.ls_g_s}\n')
            xray_source.save()

        # get TDE v.3 and to copy pre-class flags to meta object
        for meta_obj in MetaObject.objects.all():
            master_source = meta_obj.object_sources.get(survey__name=meta_obj.master_survey)
            print(f'Meta Obj {meta_obj} - master source {master_source}\n')
            meta_obj.g_s = master_source.g_s
            meta_obj.ls_g_s = master_source.ls_g_s
            meta_obj.flag_agn_wise = master_source.flag_agn_wise
            meta_obj.tde_v3 = Command.calculate_tde_v3(meta_obj)
            meta_obj.tde_v3_ls = Command.calculate_tde_v3_ls(meta_obj)
            meta_obj.save()

        self.stdout.write(f'End calculating pre-class')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Calculating took: {(end_time - start_time).total_seconds()} seconds.'))
