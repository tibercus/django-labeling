import argparse
import logging
import textwrap
from abc import ABC
from typing import Callable, Any

from django.core.management import BaseCommand
from django.utils import timezone

from surveys.models import LS
from surveys.astro import mag_ab_from_flux_nanomagies


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
        """
        Calculate AB-mags in g, r, z, w1, w2 passbands for DESI LIS objects.
        """
        all_objs = LS.objects.all()

        all_objs.update(mag_r_ab=None)
        all_objs.filter(flux_r__gt=0).update(
            mag_r_ab=mag_ab_from_flux_nanomagies("flux_r"))

        all_objs.update(mag_g_ab=None)
        all_objs.filter(flux_g__gt=0).update(
            mag_g_ab=mag_ab_from_flux_nanomagies("flux_g"))

        all_objs.update(mag_z_ab=None)
        all_objs.filter(flux_z__gt=0).update(
            mag_z_ab=mag_ab_from_flux_nanomagies("flux_z"))

        all_objs.update(mag_w1_ab=None)
        all_objs.filter(flux_w1__gt=0).update(
            mag_w1_ab=mag_ab_from_flux_nanomagies("flux_w1"))

        all_objs.update(mag_w2_ab=None)
        all_objs.filter(flux_w2__gt=0).update(
            mag_w2_ab=mag_ab_from_flux_nanomagies("flux_w2"))

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("--catalog", type=str, help="Example argument",
                            required=True)

    def handle(self, *args, **options):
        if options["catalog"].upper() == 'LS':
            self.ls()
        else:
            raise ValueError("Only --catalog LS is supported")
