from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle

from surveys.models import *
from django.contrib.auth.models import User

from django.conf import settings
import os
import datetime


class Command(BaseCommand):
    help = "Save Comments as Parquet table."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path where to save')

    @staticmethod
    def get_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at', 'source']
        return fields

    def handle(self, *args, **options):
        # self.stdout.write(f'Pandas version: {pd.__version__}')
        start_time = timezone.now()
        # file_path = options["file_path"]

        comment_df = pd.DataFrame.from_records(Comment.objects.all().values('comment', 'follow_up', 'source_class',
                                                                            'created_at', 'created_by', 'updated_at',
                                                                            'meta_source'))
        # Add metadata fields
        if not comment_df.empty:
            for i in comment_df.index:
                comment_df.at[i, 'by_user'] = User.objects.get(pk=comment_df.at[i, 'created_by']).username
                comment_df.at[i, 'master_source_name'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).master_name
                # comment_df.at[i, 'source_file'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).origin_file.file_name
                comment_df.at[i, 'master_survey'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).master_survey

            pd.set_option('display.width', 120)
            print('Saved comments:\n', comment_df)
            # add date to file name
            file_name = 'saved_comments_' + str(datetime.date.today()) + '.parquet'
            # file_name = 'saved_comments.parquet'
            comment_df.to_parquet(os.path.join(settings.WORK_DIR, file_name), engine='fastparquet')

        self.stdout.write(f'End saving comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Saving took: {(end_time - start_time).total_seconds()} seconds.'))
