from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle

from surveys.models import *
from django.contrib.auth.models import User


class Command(BaseCommand):
    help = "Save Comments as Parquet table."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path where to save')

    @staticmethod
    def get_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'source_file', 'source_row']
        return fields

    def handle(self, *args, **options):
        # self.stdout.write(f'Pandas version: {pd.__version__}')
        start_time = timezone.now()
        # file_path = options["file_path"]

        comment_df = pd.DataFrame.from_records(Comment.objects.all().values('comment', 'follow_up', 'source_class',
                                                                            'created_at', 'created_by', 'updated_at',
                                                                            'source'))
        for i in comment_df.index:
            comment_df.at[i, 'by_user'] = User.objects.get(pk=comment_df.at[i, 'created_by']).username
            comment_df.at[i, 'source_file'] = Source.objects.get(pk=comment_df.at[i, 'source']).meta_data.file_name
            comment_df.at[i, 'file_row'] = Source.objects.get(pk=comment_df.at[i, 'source']).row_num

        print(comment_df)

        self.stdout.write(f'End saving comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Saving took: {(end_time - start_time).total_seconds()} seconds.'))
