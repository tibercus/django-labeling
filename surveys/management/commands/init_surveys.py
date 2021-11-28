import csv
from django.core.management import BaseCommand
from django.utils import timezone

from surveys.models import *


class Command(BaseCommand):
    help = "Create Survey objects."

    def handle(self, *args, **options):
        start_time = timezone.now()
        for i in range(1, 10):
            # self.stdout.write(f'Create survey {i}')
            survey, create = Survey.objects.get_or_create(name=i)
            if create:
                survey.description = 'Survey' + str(i) + ' description' if i < 9 else 'Summary description'
                survey.save()

        self.stdout.write(f'End creating surveys')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Creating surveys took: {(end_time-start_time).total_seconds()} seconds.'))


