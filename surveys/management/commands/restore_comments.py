from django.core.management import BaseCommand,  CommandError
from django.utils import timezone
import pandas as pd
import pyarrow.parquet as pq

from django.conf import settings
import os

from surveys.utils import restore_comments


class Command(BaseCommand):
    help = "Restore saved comments."

    @staticmethod
    def get_saved_fields():
        fields = ['comment', 'follow_up', 'source_class', 'source_class_1', 'source_class_2', 'created_at', 'created_by',
                  'updated_at', 'meta_source', 'by_user', 'meta_ind', 'master_source_name', 'master_survey', 'RA', 'DEC']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        # Restore xray comments
        # file_name = 'saved_comments_' + str(datetime.date.today()) + '.parquet'
        file_name = 'saved_comments_2022-04-13-115632.parquet'
        if os.path.isfile(os.path.join(settings.WORK_DIR, file_name)):
            table = pq.read_table(os.path.join(settings.WORK_DIR, file_name))
            saved_comments = table.to_pandas()

            pd.set_option('display.width', 120)
            # print(saved_comments)
            restore_comments(saved_comments)

        # Restore optical comments
        # file_name = 'saved_opt_comments_' + str(datetime.date.today()) + '.parquet'
        file_name = 'saved_opt_comments_2022-04-13-115632.parquet'
        if os.path.isfile(os.path.join(settings.WORK_DIR, file_name)):
            table = pq.read_table(os.path.join(settings.WORK_DIR, file_name))
            saved_comments = table.to_pandas()

            pd.set_option('display.width', 120)
            # print('\n', saved_comments)
            restore_comments(saved_comments, 'Optical')

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
