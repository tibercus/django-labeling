from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle

from surveys.models import *
from django.contrib.auth.models import User

from django.conf import settings
import os
import datetime


def add_metadata_fields(comment_df):
    # Add metadata fields to comments table
    for i in comment_df.index:
        comment_df.at[i, 'by_user'] = User.objects.get(pk=comment_df.at[i, 'created_by']).username
        comment_df.at[i, 'meta_ind'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).meta_ind
        comment_df.at[i, 'master_source_name'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).master_name
        comment_df.at[i, 'master_survey'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).master_survey
        comment_df.at[i, 'RA'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).RA
        comment_df.at[i, 'DEC'] = MetaObject.objects.get(pk=comment_df.at[i, 'meta_source']).DEC

    return comment_df


def backup_comments():
    # Saving xray comments
    comment_df = pd.DataFrame.from_records(Comment.objects.all().values('comment', 'follow_up', 'source_class',
                                                                        'created_at', 'created_by', 'updated_at', 'meta_source'))

    if not comment_df.empty:
        comment_df = add_metadata_fields(comment_df)
        pd.set_option('display.width', 120)
        print('Saved xray comments:\n', comment_df)
        # add date to file name
        file_name = 'saved_comments_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')) + '.parquet'
        comment_df.to_parquet(os.path.join(settings.WORK_DIR, file_name), engine='fastparquet')

    # Saving optical comments
    opt_comment_df = pd.DataFrame.from_records(OptComment.objects.all().values('comment', 'follow_up', 'created_at',
                                                                               'created_by', 'updated_at', 'meta_source'))
    # Add metadata fields
    if not opt_comment_df.empty:
        opt_comment_df = add_metadata_fields(opt_comment_df)
        pd.set_option('display.width', 120)
        print('Saved optical comments:\n', opt_comment_df)
        # add date to file name
        file_name = 'saved_opt_comments_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')) + '.parquet'
        opt_comment_df.to_parquet(os.path.join(settings.WORK_DIR, file_name), engine='fastparquet')
