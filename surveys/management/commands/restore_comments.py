from django.core.management import BaseCommand,  CommandError
from django.utils import timezone
import pandas as pd
import pyarrow.parquet as pq

from surveys.models import *
from django.contrib.auth.models import User


class Command(BaseCommand):
    help = "Restore saved comments."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for parquet file')

    @staticmethod
    def get_fields():
        fields = ['comment', 'follow_up', 'source_class', 'created_at', 'created_by', 'updated_at',
                  'source', 'by_user', 'source_name', 'source_file', 'file_row']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        file_path = 'surveys/test_xray_data/saved_comments.parquet'

        table = pq.read_table(file_path)
        saved_comments = table.to_pandas()
        # print(saved_comments)

        self.stdout.write(f'Start reading saved comments')
        for row in saved_comments.itertuples():
            # Find comment's user
            try:
                user = User.objects.get(pk=row.created_by)
            except User.DoesNotExist:
                self.stdout.write(f'NOTE: User {row.by_user} not found')
                continue
            # Find source file
            try:
                meta = MetaSource.objects.get(file_name=row.source_file)
            except MetaSource.DoesNotExist:
                self.stdout.write(f'NOTE: Meta Source for {row.source_file} not found')
                continue
            # Find comment's source
            try:
                source = Source.objects.get(meta_data=meta, row_num=row.file_row)
            except Source.DoesNotExist:
                self.stdout.write(f'WARNING: Source from {row.source_file}, line: {row.source} not found')
                continue

            comment, create = Comment.objects.get_or_create(created_by=user, source=source)
            if not create:
                # skip existing comment
                self.stdout.write(f'Comment by {row.by_user} for source {source.name} exists')
                continue

            else:
                self.stdout.write(f'Restore Comment by {row.by_user} for source {source.name}'
                                  f' from file {meta.file_name}')
                comment.comment = row.comment
                comment.follow_up = row.follow_up
                comment.source_class = row.source_class
                comment.save()

        self.stdout.write(f'End restoring comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Restore Comments took: {(end_time-start_time).total_seconds()} seconds.'))
