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
    help = "Restore user saved comment on a specific meta source."

    @staticmethod
    def get_saved_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'meta_source', 'by_user', 'meta_ind', 'master_source_name', 'master_survey']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_name = 'saved_comments.parquet'

        # User request - to restore specific comment
        user_name = 'user_new'
        # TODO: think about the way to find specific comment
        # source_name = 'SRGe J192719.1+653355'
        # survey = 9
        meta_ind = 1482249
        comment_date = '2022-03-07'

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

        # Look for user comment to specific meta source
        user_source_comment = user_comments[(user_comments['meta_ind'] == meta_ind)]
        if not user_source_comment.empty:
            print('Found {} comments to meta source: {}'.format(user_name, meta_ind))
            print(user_source_comment)
        else:
            print('Cant find saved comments wrote by {} to meta source: {}'.format(user_name, meta_ind))
            print('Try to request another source or user.')
            exit()

        # Restore user comment to specific source
        self.stdout.write(f'Start restoring saved comments')
        # Find comment's user
        try:
            user = User.objects.get(username=user_name)
        except User.DoesNotExist:
            self.stdout.write(f"NOTE: User {user_name} not found")
            exit()

        # Find comment's source
        # get meta source master_name and survey
        source_name = user_source_comment['master_source_name'].values[0]
        survey = user_source_comment['master_survey'].values[0]
        try:
            meta_source = MetaObject.objects.get(meta_ind=meta_ind, master_name=source_name, master_survey=survey)
        except MetaObject.DoesNotExist:
            self.stdout.write(f"WARNING: Meta Source {user_source_comment['meta_source'].values[0]}, "
                              f"name: {source_name} survey: {survey} not found")
            exit()

        # Find comment
        try:
            comment = Comment.objects.get(created_by=user, meta_source=meta_source)
        except Comment.DoesNotExist:
            self.stdout.write(f"NOTE:Comment by {user_name} for source {meta_source.master_name} not found")
            self.stdout.write(f"NOTE:Create comment for source {meta_source.master_name} by {user_name}")
            comment = Comment.objects.create(comment='create', created_by=user, meta_source=meta_source)

        self.stdout.write(f'Restore Comment by {user_name} for source {meta_source.master_name}'
                          f' from survey {meta_source.master_survey}')
        # Fill Comment fields
        comment.comment = user_source_comment['comment'].values[0]
        comment.follow_up = user_source_comment['follow_up'].values[0]
        comment.source_class = user_source_comment['source_class'].values[0]
        # save time to comment
        comment.created_at = make_aware(pd.Timestamp(user_source_comment['created_at'].values[0]))
        comment.save()

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
