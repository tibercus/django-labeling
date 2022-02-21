from django.core.management import BaseCommand,  CommandError
from django.utils import timezone
import pandas as pd
import pyarrow.parquet as pq

from surveys.models import *
from django.contrib.auth.models import User

from django.conf import settings
import os
import datetime

from django.utils.timezone import make_aware


class Command(BaseCommand):
    help = "Restore saved comments."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for parquet file')

    @staticmethod
    def get_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'meta_source', 'by_user', 'master_source_name', 'master_survey']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_name = 'saved_comments.parquet'
        file_name = 'saved_comments_' + str(datetime.date.today()) + '.parquet'

        table = pq.read_table(os.path.join(settings.WORK_DIR, file_name))
        saved_comments = table.to_pandas()

        pd.set_option('display.width', 120)
        print(saved_comments)

        self.stdout.write(f'Start reading saved comments')
        for row in saved_comments.itertuples():
            # Find comment's user
            try:
                user = User.objects.get(username=row.by_user)
            except User.DoesNotExist:
                self.stdout.write(f'NOTE: User {row.by_user} not found')
                continue

            # Find comment's meta_source TODO: think about finding comment's meta source
            try:
                meta_source = MetaObject.objects.get(pk=row.meta_source, master_name=row.master_source_name, master_survey=row.master_survey)
            except MetaObject.DoesNotExist:
                self.stdout.write(f'WARNING: Meta Source {row.meta_source} with name {row.master_source_name}, survey: {row.master_survey} not found')
                continue

            comment, create = Comment.objects.get_or_create(created_by=user, meta_source=meta_source)
            if not create:
                # skip existing comment
                self.stdout.write(f'Comment by {row.by_user} for source {meta_source.master_name} exists')
                continue

            else:
                self.stdout.write(f'Restore Comment by {row.by_user} for meta source {meta_source.master_name}'
                                  f' survey {meta_source.master_survey}')
                try:
                    comment.comment = row.comment
                    comment.follow_up = row.follow_up
                    comment.source_class = row.source_class
                    # save time to comment
                    comment.created_at = pd.Timestamp(row.created_at)
                    comment.save()
                except Exception as e:
                    comment.delete()
                    raise e

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
