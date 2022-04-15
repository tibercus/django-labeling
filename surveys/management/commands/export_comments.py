from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle

from surveys.models import *
from django.contrib.auth.models import User

from surveys.utils import backup_comments


class Command(BaseCommand):
    help = "Save Comments as Parquet table."

    def handle(self, *args, **options):
        # self.stdout.write(f'Pandas version: {pd.__version__}')
        start_time = timezone.now()
        # file_path = options["file_path"]

        # Saving xray comments
        backup_comments()

        self.stdout.write(f'End saving comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Saving took: {(end_time - start_time).total_seconds()} seconds.'))
