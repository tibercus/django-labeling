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
        ls_sources = xray_source.ls_sources.all()
        # TODO: what to return - False or None?
        if not ls_sources.exists():
            return None
        else:
            # get ra, dec of ls sources
            ra = ls_sources.values_list('ra', flat=True)
            dec = ls_sources.values_list('dec', flat=True)
            # get sky coords for ls sources
            c_opts = SkyCoord(ra=ra*u.degree, dec=dec*u.degree, distance=1*u.pc, frame='icrs')
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
    def calculate_tde_v3(meta_object, master_source):
        pass

    def handle(self, *args, **options):
        start_time = timezone.now()
        for xray_source in eROSITA.objects.all():
            # calculate radius of correlation
            Rc = np.clip(1.1 * xray_source.pos_r98, 4, 20)
            # get SkyCoord for calculating separation
            c_xray = SkyCoord(ra=xray_source.RA*u.degree, dec=xray_source.DEC*u.degree, distance=1*u.pc, frame='icrs')

            # get flag values
            if xray_source.flag_agn_wise is None:
                print(f'Radius of correlation:{Rc}')
                xray_source.flag_agn_wise = Command.calculate_wise_agn(xray_source, c_xray, Rc)
                print(f'AGN Flag for {xray_source.survey.name} {xray_source}: {xray_source.flag_agn_wise}\n')

            if xray_source.g_s is None:
                print(f'Radius of correlation:{Rc}')
                xray_source.g_s = Command.calculate_gaia_star(xray_source, c_xray, Rc)
                print(f'GAIA Star Flag for {xray_source.survey.name} {xray_source}: {xray_source.g_s}\n')
            xray_source.save()

        for meta_obj in MetaObject.objects.all():
            master_source = meta_obj.object_sources.get(survey__name=meta_obj.master_survey)
            # meta_obj.tde_v3 = Command.calculate_tde_v3(meta_obj, master_source)
            # meta_obj.save()

        self.stdout.write(f'End calculating pre-class')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Calculating took: {(end_time-start_time).total_seconds()} seconds.'))
