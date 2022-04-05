from django.core.management import BaseCommand
from django.utils import timezone

from surveys.models import *


class Command(BaseCommand):
    help = "Clear Optical Surveys - Delete Sources."

    def handle(self, *args, **options):
        start_time = timezone.now()

        print('Delete LS sources')
        ls_sources = LS.objects.all()
        ls_sources.delete()

        print('Delete SDSS sources')
        sdss_sources = SDSS.objects.all()
        sdss_sources.delete()

        print('Delete PS sources')
        ps_sources = PS.objects.all()
        ps_sources.delete()

        print('Delete GAIA sources')
        gaia_sources = GAIA.objects.all()
        gaia_sources.delete()

        # clear eROSITA sources opt fields
        print('Delete optical fields of eROSITA sources')
        sources = eROSITA.objects.all()
        sources.update(ls_dup_sep=None, sdss_dup_sep=None, ps_dup_sep=None, gaia_dup_sep=None)
        # sources.save()

        self.stdout.write(f'End clearing surveys')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Clearing took: {(end_time-start_time).total_seconds()} seconds.'))
