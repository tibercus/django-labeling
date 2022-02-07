from django.core.management import BaseCommand
from django.utils import timezone

from surveys.models import *


class Command(BaseCommand):
    help = "Clear Surveys - Delete Sources."

    def handle(self, *args, **options):
        start_time = timezone.now()

        print('Delete sources')
        sources = Source.objects.all()
        sources.delete()

        print('Delete meta objects')
        meta_objects = MetaObject.objects.all()
        meta_objects.delete()

        print('Delete meta sources')
        meta_sources = MetaSource.objects.all()
        meta_sources.delete()

        self.stdout.write(f'End clearing surveys')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Clearing took: {(end_time-start_time).total_seconds()} seconds.'))
