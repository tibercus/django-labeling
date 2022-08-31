import textwrap
from typing import Type, List, Callable

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

from django.db.models import ExpressionWrapper, FloatField, Model
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

def get_sep(master_source, opt_source):
    """get separation between master source and optical source"""
    c_xray = SkyCoord(ra=master_source.RA*u.degree, dec=master_source.DEC*u.degree, distance=1*u.pc, frame='icrs')
    c_opt = SkyCoord(ra=opt_source.ra*u.degree, dec=opt_source.dec*u.degree, distance=1*u.pc, frame='icrs')
    sep = c_xray.separation(c_opt)
    return sep.arcsecond

def change_opt_cp(source, opt_survey_name, opt_id):
    opt_sources = eROSITA.get_opt_survey_sources(source, opt_survey_name)
    if opt_sources is None:
        return 'No opt sources in this survey'

    new_opt_cp = opt_sources.filter(opt_id=opt_id)
    if not new_opt_cp.exists():
        return 'No opt sources with this opt_id'

    new_opt_cp = new_opt_cp[0]
    new_sep = get_sep(source, new_opt_cp)
    print('Opt source:', new_opt_cp, 'sep:', new_sep)
    eROSITA.change_dup_source(source, opt_survey_name, new_opt_cp, new_sep)
    return 1



def backup_comments():
    # Saving xray comments
    comment_df = pd.DataFrame.from_records(Comment.objects.all().values('comment', 'follow_up',
                                                                        'source_class', 'source_class_1', 'source_class_2',
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


def string_representation(include_fields: List[str] = None,
                          exclude_fields: List[str] = None,
                          oneline: bool = False):
    """Generates __str__ method for Django model.

    :param include_fields: List of fields names to add to string
    representation.
    :param exclude_fields: List of fields names to exclude from string
    representation.
    :param oneline: if true, then will generate representation like a
    constructor call. Else, multiline, like class description in Python.

    :return: Django model decorator.

    Does not work in following cases:
    TODO model is inherited from user-defined model and include_fields is None
    TODO recursive FK
    """
    exclude_fields = exclude_fields or list()

    def decorator(model: Type[Model]) -> Type[Model]:

        def __f(__o: Model) -> str:
            beginning = f"{__o.__class__.__name__}"
            if oneline:
                beginning += "("
                delimiter = ", "
                ending = ")"
            else:
                beginning += ":\n"
                delimiter = "\n"
                ending = "\n"

            if include_fields is None:
                fields_names = [f.name for f in model._meta.get_fields()]
            else:
                fields_names = include_fields

            values = delimiter.join(
                [f"{f}={getattr(__o, f)}" for f in fields_names
                 if f not in exclude_fields])

            if not oneline:
                values = textwrap.indent(values, " "*4)

            return beginning + values + ending

        setattr(model, "__str__", __f)
        return model

    return decorator


def help_from_docstring(model: Type[BaseCommand]):
    """Set's class `help` attribute equal to `__doc__`."""
    setattr(model, "help", textwrap.dedent(model.__doc__))
    return model
