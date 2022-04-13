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

    def restore_comments(self, saved_comments, com_type='Xray'):

        self.stdout.write(f'Start reading saved comments')
        for row in saved_comments.itertuples():
            # Find comment's user
            try:
                user = User.objects.get(username=row.by_user)
            except User.DoesNotExist:
                self.stdout.write(f'NOTE: User {row.by_user} not found')
                continue

            # TODO: think about finding comment's meta source by meta_ind
            # Find comment's meta_source
            try:
                meta_source = MetaObject.objects.get(meta_ind=row.meta_ind, master_name=row.master_source_name,
                                                     master_survey=row.master_survey)
            except MetaObject.DoesNotExist:
                self.stdout.write(
                    f'WARNING: Meta Source {row.meta_ind} with name {row.master_source_name}, survey: {row.master_survey} not found')
                continue

            # Find/create comment
            if com_type == 'Xray':
                comment, create = Comment.objects.get_or_create(created_by=user, meta_source=meta_source)
            else:
                comment, create = OptComment.objects.get_or_create(created_by=user, meta_source=meta_source)

            if not create:
                # skip existing comment
                self.stdout.write(f'{com_type} Comment by {row.by_user} for source {meta_source.master_name} exists')
                continue
            else:
                self.stdout.write(f'Restore {com_type} Comment by {row.by_user} for meta source {meta_source.master_name}'
                                  f' survey {meta_source.master_survey}')
                # Fill Comment fields
                try:
                    comment.comment = row.comment
                    comment.follow_up = row.follow_up
                    if com_type == 'Xray': comment.source_class = row.source_class
                    # save time to comment
                    comment.created_at = pd.Timestamp(row.created_at)
                    comment.save()
                except Exception as e:
                    comment.delete()
                    self.stdout.write(
                        f'ERROR: Restoring {com_type} Comment by {row.by_user} for meta source {meta_source.master_name}'
                        f' survey {meta_source.master_survey}')
                    raise e

    @staticmethod
    def get_saved_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'meta_source', 'by_user', 'meta_ind', 'master_source_name', 'master_survey', 'RA', 'DEC']
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
            print(saved_comments)
            Command.restore_comments(self, saved_comments)

        # Restore optical comments
        # file_name = 'saved_opt_comments_' + str(datetime.date.today()) + '.parquet'
        file_name = 'saved_opt_comments_2022-04-13-115632.parquet'
        if os.path.isfile(os.path.join(settings.WORK_DIR, file_name)):
            table = pq.read_table(os.path.join(settings.WORK_DIR, file_name))
            saved_comments = table.to_pandas()

            pd.set_option('display.width', 120)
            print('\n', saved_comments)
            Command.restore_comments(self, saved_comments, 'Optical')

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
