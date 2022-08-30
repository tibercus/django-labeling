import argparse
import logging
import textwrap
from abc import ABC
from typing import Callable, Any

from django.core.management import BaseCommand, CommandError
from django.utils import timezone
from django.db.models import Q

from surveys.models import LS, PS, SDSS
import surveys.astro as saf


def timeit(job_name: str) -> Callable:
    def decorator(function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            start_time = timezone.now()
            logging.info(f"{start_time} - {job_name} started.")

            # TODO add exception handling for full logging.
            result = function(*args, **kwargs)

            finish_time = timezone.now()
            logging.info(f"{finish_time} - {job_name} ended.")
            logging.info(
                f"{finish_time} - "
                f"Took {(finish_time - start_time).total_seconds()} seconds."
            )
            return result

        return wrapper
    return decorator


class BaseCommandWithFormattedHelp(BaseCommand, ABC):
    def create_parser(self, *args, **kwargs):
        parser = super().create_parser(*args, **kwargs)
        parser.formatter_class = argparse.RawTextHelpFormatter
        return parser


class Command(BaseCommandWithFormattedHelp):
    """
    A command to calculate additional source attributes not present in raw
    data. List of attributes:

    - TODO
    """
    help = textwrap.dedent(__doc__)

    @staticmethod
    @timeit("DESI LIS magnitude calculation")
    def ls():
        """Calculate AB-mags in g, r, z, w1, w2 for DESI LIS objects.
        """
        all_objs = LS.objects.all()

        all_objs.update(mag_r_ab=None)
        all_objs.update(mag_err_r_ab=None)
        all_objs.filter(flux_r__gt=0).update(
            mag_r_ab=saf.mag_ab_from_flux_nanomagies("flux_r")
        )
        all_objs.filter(Q(flux_r__gt=0) & Q(flux_ivar_r__gt=0)).update(
            mag_err_r_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_r", "flux_ivar_r"
            )
        )

        all_objs.update(mag_g_ab=None)
        all_objs.update(mag_err_g_ab=None)
        all_objs.filter(flux_g__gt=0).update(
            mag_g_ab=saf.mag_ab_from_flux_nanomagies("flux_g")
        )
        all_objs.filter(Q(flux_g__gt=0) & Q(flux_ivar_g__gt=0)).update(
            mag_err_g_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_g", "flux_ivar_g"
            )
        )

        all_objs.update(mag_z_ab=None)
        all_objs.update(mag_err_z_ab=None)
        all_objs.filter(flux_z__gt=0).update(
            mag_z_ab=saf.mag_ab_from_flux_nanomagies("flux_z")
        )
        all_objs.filter(Q(flux_z__gt=0) & Q(flux_ivar_z__gt=0)).update(
            mag_err_z_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_z", "flux_ivar_z"
            )
        )

        all_objs.update(mag_w1_ab=None)
        all_objs.update(mag_err_w1_ab=None)
        all_objs.filter(flux_w1__gt=0).update(
            mag_w1_ab=saf.mag_ab_from_flux_nanomagies("flux_w1")
        )
        all_objs.filter(Q(flux_w1__gt=0) & Q(flux_ivar_w1__gt=0)).update(
            mag_err_w1_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_w1", "flux_ivar_w1"
            )
        )

        all_objs.update(mag_w2_ab=None)
        all_objs.update(mag_err_w2_ab=None)
        all_objs.filter(flux_w2__gt=0).update(
            mag_w2_ab=saf.mag_ab_from_flux_nanomagies("flux_w2")
        )
        all_objs.filter(Q(flux_w2__gt=0) & Q(flux_ivar_w2__gt=0)).update(
            mag_err_w2_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_w2", "flux_ivar_w2"
            )
        )

        all_objs.update(mag_w3_ab=None)
        all_objs.update(mag_err_w3_ab=None)
        all_objs.filter(flux_w3__gt=0).update(
            mag_w3_ab=saf.mag_ab_from_flux_nanomagies("flux_w3")
        )
        all_objs.filter(Q(flux_w3__gt=0) & Q(flux_ivar_w3__gt=0)).update(
            mag_err_w3_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_w3", "flux_ivar_w3"
            )
        )

        all_objs.update(mag_w4_ab=None)
        all_objs.update(mag_err_w4_ab=None)
        all_objs.filter(flux_w4__gt=0).update(
            mag_w4_ab=saf.mag_ab_from_flux_nanomagies("flux_w4")
        )
        all_objs.filter(Q(flux_w4__gt=0) & Q(flux_ivar_w4__gt=0)).update(
            mag_err_w4_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "flux_w4", "flux_ivar_w4"
            )
        )

    @staticmethod
    @timeit("Pan-STARRS magnitudes calculation")
    def ps():
        """Calculate g, r, i, z, y AB-magnitudes and errors from Pan-STARRS
        flux (Jy) and ivar.
        """
        all_objs = PS.objects.all()

        all_objs.update(gKronMagAB=None)
        all_objs.update(yKronMagErrAB=None)
        all_objs.filter(gKronFlux__gt=0).update(
            gKronMagAB=saf.mag_ab_from_flux_jy("gKronFlux")
        )
        all_objs.filter(Q(gKronFlux__gt=0) & Q(gKronFluxErr__gt=0)).update(
            gKronMagErrAB=saf.mag_err_ab_from_flux_jy(
                "gKronFlux", "gKronFluxErr"
            )
        )

        all_objs.update(rKronMagAB=None)
        all_objs.update(yKronMagErrAB=None)
        all_objs.filter(rKronFlux__gt=0).update(
            rKronMagAB=saf.mag_ab_from_flux_jy("rKronFlux")
        )
        all_objs.filter(Q(rKronFlux__gt=0) & Q(rKronFluxErr__gt=0)).update(
            rKronMagErrAB=saf.mag_err_ab_from_flux_jy(
                "rKronFlux", "rKronFluxErr"
            )
        )

        all_objs.update(iKronMagAB=None)
        all_objs.update(yKronMagErrAB=None)
        all_objs.filter(iKronFlux__gt=0).update(
            iKronMagAB=saf.mag_ab_from_flux_jy("iKronFlux")
        )
        all_objs.filter(Q(iKronFlux__gt=0) & Q(iKronFluxErr__gt=0)).update(
            iKronMagErrAB=saf.mag_err_ab_from_flux_jy(
                "iKronFlux", "iKronFluxErr"
            )
        )

        all_objs.update(zKronMagAB=None)
        all_objs.update(yKronMagErrAB=None)
        all_objs.filter(zKronFlux__gt=0).update(
            zKronMagAB=saf.mag_ab_from_flux_jy("zKronFlux")
        )
        all_objs.filter(Q(zKronFlux__gt=0) & Q(zKronFluxErr__gt=0)).update(
            zKronMagErrAB=saf.mag_err_ab_from_flux_jy(
                "zKronFlux", "zKronFluxErr"
            )
        )

        all_objs.update(yKronMagAB=None)
        all_objs.update(yKronMagErrAB=None)
        all_objs.filter(yKronFlux__gt=0).update(
            yKronMagAB=saf.mag_ab_from_flux_jy("yKronFlux")
        )
        all_objs.filter(Q(yKronFlux__gt=0) & Q(yKronFluxErr__gt=0)).update(
            yKronMagErrAB=saf.mag_err_ab_from_flux_jy(
                "yKronFlux", "yKronFluxErr"
            )
        )

    @staticmethod
    @timeit("SDSS magnitudes calculation")
    def sdss():
        """Calculate AB-mags in u, g, r, i, z for SDSS objects.
        """
        all_objs = SDSS.objects.all()

        all_objs.update(cModelMag_u_ab=None)
        all_objs.update(cModelMagErr_u_ab=None)
        all_objs.filter(cModelFlux_u__gt=0).update(
            cModelMag_u_ab=saf.mag_ab_from_flux_nanomagies("cModelFlux_u")
        )
        all_objs.filter(
            Q(cModelFlux_u__gt=0) & Q(cModelFluxIvar_u__gt=0)
        ).update(
            cModelMagErr_u_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "cModelFlux_u", "cModelFluxIvar_u"
            )
        )

        all_objs.update(cModelMag_g_ab=None)
        all_objs.update(cModelMagErr_g_ab=None)
        all_objs.filter(cModelFlux_g__gt=0).update(
            cModelMag_g_ab=saf.mag_ab_from_flux_nanomagies("cModelFlux_g")
        )
        all_objs.filter(
            Q(cModelFlux_g__gt=0) & Q(cModelFluxIvar_g__gt=0)
        ).update(
            cModelMagErr_g_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "cModelFlux_g", "cModelFluxIvar_g"
            )
        )

        all_objs.update(cModelMag_r_ab=None)
        all_objs.update(cModelMagErr_r_ab=None)
        all_objs.filter(cModelFlux_r__gt=0).update(
            cModelMag_r_ab=saf.mag_ab_from_flux_nanomagies("cModelFlux_r")
        )
        all_objs.filter(
            Q(cModelFlux_r__gt=0) & Q(cModelFluxIvar_r__gt=0)
        ).update(
            cModelMagErr_r_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "cModelFlux_r", "cModelFluxIvar_r"
            )
        )

        all_objs.update(cModelMag_i_ab=None)
        all_objs.update(cModelMagErr_i_ab=None)
        all_objs.filter(cModelFlux_i__gt=0).update(
            cModelMag_i_ab=saf.mag_ab_from_flux_nanomagies("cModelFlux_i")
        )
        all_objs.filter(
            Q(cModelFlux_i__gt=0) & Q(cModelFluxIvar_i__gt=0)
        ).update(
            cModelMagErr_i_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "cModelFlux_i", "cModelFluxIvar_i"
            )
        )

        all_objs.update(cModelMag_z_ab=None)
        all_objs.update(cModelMagErr_z_ab=None)
        all_objs.filter(cModelFlux_z__gt=0).update(
            cModelMag_z_ab=saf.mag_ab_from_flux_nanomagies("cModelFlux_z")
        )
        all_objs.filter(
            Q(cModelFlux_z__gt=0) & Q(cModelFluxIvar_z__gt=0)
        ).update(
            cModelMagErr_z_ab=saf.mag_err_ab_from_flux_nanomaggies(
                "cModelFlux_z", "cModelFluxIvar_z"
            )
        )

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("--catalog", type=str, help="LS, PS, SDSS or ALL",
                            required=True)

    @staticmethod
    def validate_arguments(*args, **options):
        if options["catalog"].upper() not in ("LS", "PS", "SDSS", "ALL"):
            raise CommandError("Only --catalog 'LS', 'PS' or 'SDSS' is "
                               "supported")

    def handle(self, *args, **options):
        self.validate_arguments(*args, **options)

        catalog = options["catalog"].upper()
        run_all = catalog == "ALL"

        if catalog == "LS" or run_all:
            self.ls()

        if catalog in "PS" or run_all:
            self.ps()

        if catalog == "SDSS" or run_all:
            self.sdss()
