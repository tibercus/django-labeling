from django.core.management import BaseCommand,  CommandError
from django.utils import timezone
import pandas as pd
import pyarrow.parquet as pq

from surveys.models import *
from django.contrib.auth.models import User

from django.conf import settings
import os
from django.utils.timezone import make_aware

class Command(BaseCommand):
    help = "Restore user saved comment on a specific source."

    @staticmethod
    def get_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'source', 'by_user', 'source_name', 'source_file', 'file_row']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_name = 'saved_comments.parquet'

        # User request - to restore specific comment
        user_name = 'admin'
        source_name = 'SRGe J023517.2+052736'
        comment_date = '2022-01-29'

        file_name = 'saved_comments_' + comment_date + '.parquet'
        # Look for saved comments with request date
        if os.path.isfile(os.path.join(settings.WORK_DIR, file_name)):
            print('Found saved comments dated '+comment_date)
            table = pq.read_table(os.path.join(settings.WORK_DIR, file_name))
            saved_comments = table.to_pandas()
        else:
            print('Cant find saved comments dated ' + comment_date)
            print('Try to request another date.')
            exit()

        pd.set_option('display.width', 150)
        print(saved_comments)

        # Look for user comments
        user_comments = saved_comments[saved_comments['by_user'] == user_name]
        if not user_comments.empty:
            print('Found comments by {}:'.format(user_name))
            print(user_comments)
        else:
            print('Cant find saved comments wrote by ' + user_name)
            print('Try to request another user.')
            exit()

        # Look for user comment to specific source
        user_source_comment = user_comments[user_comments['source_name'] == source_name]
        if not user_source_comment.empty:
            print('Found {} comments to source: {}'.format(user_name, source_name))
            print(user_source_comment)
        else:
            print('Cant find saved comments wrote by {} to source: {}'.format(user_name, source_name))
            print('Try to request another source or user.')
            exit()

        # Restore user comment to specific source
        self.stdout.write(f'Start restoring saved comments')
        # Find comment's user
        try:
            user = User.objects.get(pk=user_source_comment['created_by'].values[0])
        except User.DoesNotExist:
            self.stdout.write(f"NOTE: User {user_name} not found")
            exit()

        # Find source file
        try:
            meta = MetaSource.objects.get(file_name=user_source_comment['source_file'].values[0])
        except MetaSource.DoesNotExist:
            self.stdout.write(f"NOTE: Meta Source for {user_source_comment['source_file']} not found")
            exit()

        # Find comment's source
        try:
            source = Source.objects.get(name=source_name, meta_data=meta, row_num=user_source_comment['file_row'].values[0])
        except Source.DoesNotExist:
            self.stdout.write(f"WARNING: Source from {user_source_comment['source_file'].values[0]}, "
                              f"line: {user_source_comment['file_row'].values[0]} not found")
            exit()

        # Find comment
        try:
            comment = Comment.objects.get(created_by=user, source=source)
        except Comment.DoesNotExist:
            self.stdout.write(f"WARNING:Comment by {user_name} for source {source.name} not found")
            exit()

        self.stdout.write(f'Restore Comment by {user_name} for source {source.name}'
                          f' from file {meta.file_name}')
        # fill Comment fields
        comment.comment = user_source_comment['comment'].values[0]
        comment.follow_up = user_source_comment['follow_up'].values[0]
        comment.source_class = user_source_comment['source_class'].values[0]
        # save time to comment
        comment.created_at = make_aware(pd.Timestamp(user_source_comment['created_at'].values[0]))
        comment.save()

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
