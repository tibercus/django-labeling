from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle
import pyarrow.parquet as pq

from surveys.models import *
from django.contrib.auth.models import User

from django.conf import settings
import os
import datetime

from django.utils.timezone import make_aware

from django.db.models import ExpressionWrapper, FloatField
from django.db.models.functions.math import ACos, Cos, Radians, Pi, Sin
from math import radians


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
        # pd.set_option('display.width', 120)
        # print('Saved xray comments:\n', comment_df)
        # add date to file name
        file_name = 'saved_comments_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')) + '.parquet'
        comment_df.to_parquet(os.path.join(settings.WORK_DIR, file_name), engine='fastparquet')

    # Saving optical comments
    opt_comment_df = pd.DataFrame.from_records(OptComment.objects.all().values('comment', 'follow_up', 'created_at',
                                                                               'created_by', 'updated_at', 'meta_source'))
    # Add metadata fields
    if not opt_comment_df.empty:
        opt_comment_df = add_metadata_fields(opt_comment_df)
        # pd.set_option('display.width', 120)
        # print('Saved optical comments:\n', opt_comment_df)
        # add date to file name
        file_name = 'saved_opt_comments_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')) + '.parquet'
        opt_comment_df.to_parquet(os.path.join(settings.WORK_DIR, file_name), engine='fastparquet')


def restore_comments(saved_comments, com_type='Xray'):
    print(f'Start reading saved comments')
    for row in saved_comments.itertuples():
        # Find comment's user
        try:
            user = User.objects.get(username=row.by_user)
        except User.DoesNotExist:
            print(f'WARNING: User {row.by_user} not found')
            continue

        # TODO: think about finding comment's meta source by meta_ind
        # Find comment's meta_source
        try:
            meta_source = MetaObject.objects.get(meta_ind=row.meta_ind, master_name=row.master_source_name,
                                                 master_survey=row.master_survey)
        except MetaObject.DoesNotExist:
            print(f'WARNING: Meta Source {row.meta_ind} with name {row.master_source_name}, survey: {row.master_survey} not found')
            continue

        # Find/create comment
        if com_type == 'Xray':
            comment, create = Comment.objects.get_or_create(created_by=user, meta_source=meta_source)
        else:
            comment, create = OptComment.objects.get_or_create(created_by=user, meta_source=meta_source)

        if not create:
            # skip existing comment
            print(f'{com_type} Comment by {row.by_user} for source {meta_source.master_name} exists')
            continue
        else:
            print(f'Restore {com_type} Comment by {row.by_user} for meta source {meta_source.master_name}'
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
                print(
                    f'ERROR: Restoring {com_type} Comment by {row.by_user} for meta source {meta_source.master_name}'
                    f' survey {meta_source.master_survey}')
                raise e


def cone_search_filter(queryset, ra, dec, radius):
    """
    Executes cone search by annotating each target with separation distance from the specified RA/Dec.
    Formula is from Wikipedia: https://en.wikipedia.org/wiki/Angular_distance
    The result is converted to radians.

    :param queryset: Queryset of Target objects
    :type queryset: Target

    :param ra: Right ascension of center of cone.
    :type ra: float

    :param dec: Declination of center of cone.
    :type dec: float

    :param radius: Radius of cone search in degrees.
    :type radius: float
    """
    ra = float(ra)
    dec = float(dec)
    radius = float(radius)

    # Cone search is preceded by a square search to reduce the search radius before annotating the queryset, in
    # order to make the query faster.
    double_radius = radius * 2
    queryset = queryset.filter(
        RA__gte=ra - double_radius, RA__lte=ra + double_radius,
        DEC__gte=dec - double_radius, DEC__lte=dec + double_radius
    )

    separation = ExpressionWrapper(
        180/Pi() * ACos(
            (Sin(radians(dec)) * Sin(Radians('DEC'))) +
            (Cos(radians(dec)) * Cos(Radians('DEC')) * Cos(radians(ra) - Radians('RA')))
        ), FloatField()
    )

    return queryset.annotate(separation=separation).filter(separation__lte=radius)
