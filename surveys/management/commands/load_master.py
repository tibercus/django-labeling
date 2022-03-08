from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from surveys.models import *
from django.conf import settings
import shutil
import os

import astropy.units as u
from astropy.coordinates import SkyCoord

from django.db.models import Max
from django.db.models import Count


class Command(BaseCommand):
    help = "Loads data from Parquet file."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for parquet file')

    @staticmethod
    def get_fields():  # add img_id to identify images in load_data
        fields = ['meta_ind', 'RA', 'DEC', 'unchange_flag', 'comment', 'object_class', 'EXT', 'R98', 'LIKE',
                  'D2D_e1m', 'D2D_e2m', 'D2D_e3m', 'D2D_e4m', 'D2D_e5m', 'D2D_me1', 'D2D_me2', 'D2D_me3', 'D2D_me4', 'D2D_me5',
                  'EXP_e1', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e5', 'EXP_e1234',
                  'ID_FLAG_e1m', 'ID_FLAG_e2m', 'ID_FLAG_e3m', 'ID_FLAG_e4m', 'ID_FLAG_e5m',
                  'ID_FLAG_me1', 'ID_FLAG_me2', 'ID_FLAG_me3', 'ID_FLAG_me4', 'ID_FLAG_me5',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e5', 'ID_e1234',
                  'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3', 'RATIO_e5e4', 'RFLAG_e2e1', 'RFLAG_e3e2', 'RFLAG_e4e3', 'RFLAG_e5e4',
                  'R_NSRC_e1m', 'R_NSRC_e2m', 'R_NSRC_e3m', 'R_NSRC_e4m', 'R_NSRC_e5m',
                  'R_NSRC_me1', 'R_NSRC_me2', 'R_NSRC_me3', 'R_NSRC_me4', 'R_NSRC_me5',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e5', 'UPLIM_e1234', 'flag',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTART_e5',
                  'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4', 'TSTOP_e5']
        return fields

    @staticmethod
    def rename_copy_images(img_id, meta_ind):
        # Rename and Copy images to dir: static/images
        # img_id, master_name - attributes of master table

        images_path = settings.MASTER_DIR
        for i in range(1, 10):
            # Copy Light Curve
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_lc' + '.pdf')
            if os.path.isfile(old_path):
                new_file_name = 'lc_' + str(meta_ind) + '.pdf'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

            # Copy Spectrum
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_spec' + '.pdf')
            if os.path.isfile(old_path):
                new_file_name = 'spec_' + str(meta_ind) + '.pdf'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

            # Copy Trans Image
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_e' + str(i) + '.png')
            if os.path.isfile(old_path):
                new_file_name = 'trans_' + str(meta_ind) + '.png'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

    @staticmethod
    def link_source_with_meta(meta_object, row_values):
        if row_values.ID_e1 > 0 and row_values.ID_e1 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e1, survey=Survey.objects.get(name=1))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e1} from survey 1 not found')

        if row_values.ID_e2 and row_values.ID_e2 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e2, survey=Survey.objects.get(name=2))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e2} from survey 2 not found')

        if row_values.ID_e3 and row_values.ID_e3 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e3, survey=Survey.objects.get(name=3))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e3} from survey 3 not found')

        if row_values.ID_e4 and row_values.ID_e4 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e4, survey=Survey.objects.get(name=4))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e4} from survey 4 not found')

        if row_values.ID_e5 and row_values.ID_e5 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e5, survey=Survey.objects.get(name=5))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e5} from survey 5 not found')

        if row_values.ID_e1234 and row_values.ID_e1234 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e1234, survey=Survey.objects.get(name=9))
                source.meta_objects.add(meta_object)
                print(f'Link meta object: {meta_object} with source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e1234} from survey 9 not found')

    @staticmethod
    def find_master_source(meta_object):
        sources = meta_object.object_sources.all()
        if sources:
            max_dl0 = sources.aggregate(Max('DET_LIKE_0'))['DET_LIKE_0__max']
            master_source = sources.get(DET_LIKE_0=max_dl0)
            print(f'Found master_source  - {master_source.name} with DET_LIKE_0: {max_dl0} survey: {master_source.survey.name}')
            # take name, survey, RA, DEC, EXT, R98, LIKE from master_source
            meta_object.master_name = master_source.name
            meta_object.master_survey = master_source.survey.name
            meta_object.RA = master_source.RA
            meta_object.DEC = master_source.DEC
            meta_object.EXT = master_source.EXT
            meta_object.R98 = master_source.pos_r98
            meta_object.LIKE = master_source.DET_LIKE_0
            meta_object.save()

    @staticmethod
    def create_m_group_with_prime_obj(prime_obj):
        meta_group = MetaGroup.objects.create(meta_ind=prime_obj.meta_ind,
                                              master_name=prime_obj.master_name,
                                              master_survey=prime_obj.master_survey,
                                              max_sources_num=prime_obj.object_sources.count())
        print(f'Create new meta group {meta_group}\n')
        prime_obj.meta_group = meta_group
        prime_obj.save()
        return meta_group

    @staticmethod
    def change_prime_object(meta_group, new_prime_obj):
        # change flag of old primary object
        old_primary_obj = meta_group.meta_objects.get(primary_object=True)
        old_primary_obj.primary_object = False
        old_primary_obj.save()
        # handle new primary object
        meta_group.meta_ind = new_prime_obj.meta_ind
        meta_group.master_name = new_prime_obj.master_name
        meta_group.master_survey = new_prime_obj.master_survey
        meta_group.max_sources_num = new_prime_obj.object_sources.count()
        meta_group.save()
        # change meta object flag
        new_prime_obj.primary_object = True
        return meta_group

    @staticmethod
    def union_meta_groups(meta_object, meta_groups):
        # get meta group with max sources number
        print(f'Choose primary object and join meta_groups: {meta_groups}')
        max_meta_group = meta_groups.order_by('-max_sources_num')[0]
        other_meta_groups = meta_groups.exclude(pk=max_meta_group.pk)
        print(f'Found max source num meta group: {max_meta_group}')
        print(
            f'Meta_object source_num: {meta_object.object_sources.count()}, meta_group: {max_meta_group.max_sources_num}')
        if max_meta_group.max_sources_num < meta_object.object_sources.count():
            # make meta object - primary object for max meta group
            Command.change_prime_object(max_meta_group, meta_object)
        else:
            meta_object.primary_object = False

        # join meta group
        meta_object.meta_group = max_meta_group
        meta_object.save()
        # change meta group and flag of meta objects from other meta groups
        for meta_group in other_meta_groups.distinct():
            other_meta_objects = meta_group.meta_objects.all()
            other_meta_objects.update(meta_group=max_meta_group, primary_object=False)

        # delete other meta groups
        print(f'Delete meta groups: {other_meta_groups}\n')
        other_meta_groups.delete()

    @staticmethod
    def find_or_create_meta_group(meta_object):
        print(f'Find or create meta group for MetaObj: {meta_object}')
        # dict with id: number of related source
        sources_num = {meta_object.pk: meta_object.object_sources.count()}
        sources = meta_object.object_sources.all()
        # create empty queryset
        dup_objects = MetaObject.objects.none()
        for source in sources:
            # union all linked meta_objects
            dup_objects = dup_objects | source.meta_objects.all()

        # get distinct queryset of dup meta objects without current object
        dup_objects = dup_objects.distinct()
        dup_objects_ = dup_objects.exclude(pk=meta_object.pk)
        print(f'\n Get all meta objects with common sources: {dup_objects}')

        # queryset is empty -> no meta objects with common sources
        if not dup_objects_:
            Command.create_m_group_with_prime_obj(meta_object)
            return

        # find all meta groups for dup_objects
        meta_groups = MetaGroup.objects.none()
        list_of_ids = []
        for dup_obj in dup_objects_:
            # get id: number of related sources for all dup meta objects
            sources_num[dup_obj.pk] = dup_obj.object_sources.count()
            # union all linked meta_groups
            if dup_obj.meta_group: list_of_ids.append(dup_obj.meta_group.pk)

        meta_groups = MetaGroup.objects.filter(pk__in=list_of_ids) if list_of_ids else MetaGroup.objects.none()
        meta_groups_dist = meta_groups.distinct() if meta_groups else meta_groups
        print(f'Get all meta groups for dup_objects: {meta_groups_dist}\n')

        # queryset is empty -> no meta groups for meta_objects with common sources
        # create meta group for these meta objects
        if not meta_groups.exists():
            # get meta_ind and sources number of primary meta object
            prime_pk = max(sources_num, key=sources_num.get)
            prime_object = dup_objects.get(pk=prime_pk)
            print(f'Found prime object {prime_object} with max sources number: {sources_num[prime_pk]}')
            meta_group = Command.create_m_group_with_prime_obj(prime_object)
            dup_objects.exclude(pk=prime_object.pk).update(meta_group=meta_group, primary_object=False)
            return

        # there is one meta group -> join it and check primary object
        if len(meta_groups_dist) == 1:
            meta_group = meta_groups_dist[0]
            print(f'Found meta group: {meta_group} for {meta_object}')
            print(f'Meta_object source_num: {meta_object.object_sources.count()}, meta_group: {meta_group.max_sources_num}\n')
            if meta_group.max_sources_num < meta_object.object_sources.count():
                # make meta object - primary object for meta group
                Command.change_prime_object(meta_group, meta_object)
            else:
                meta_object.primary_object = False

            # join meta group
            meta_object.meta_group = meta_group
            meta_object.save()
            return
        # there is several meta groups - > choose new primary source and union meta groups
        else:
            Command.union_meta_groups(meta_object, meta_groups)
            return

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_path = options["file_path"]
        file_path = os.path.join(settings.WORK_DIR, 'master_sources.parquet')

        # Field list in the order in which the columns should be in the table
        field_list = Command.get_fields()

        table = pq.read_table(file_path)
        data = table.to_pandas()
        # replace all nan with None
        data = data.replace({np.nan: None})

        # Make Survey dirs for image data
        for i in range(1, 10):
            # check path
            print(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i)))
            if not os.path.exists(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i))):
                os.makedirs(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i)))

        self.stdout.write(f'Start reading data')
        # sources = []
        print(data)

        for row in data.itertuples():
            # find/create meta object
            meta_object, created = MetaObject.objects.get_or_create(ID_e1=row.ID_e1, ID_e2=row.ID_e2, ID_e3=row.ID_e3,
                                                                    ID_e4=row.ID_e4, ID_e5=row.ID_e5, ID_e1234=row.ID_e1234,
                                                                    defaults={'RA': row.RA, 'DEC': row.DEC})

            # Check that it is new meta object
            if created:
                print(f'{row[0]} - Create new meta object with img_id: {row.img_id}, RA: {row.RA}, DEC: {row.DEC}')
                try:
                    self.stdout.write(f'Start filling fields...\n')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+1]}')  # i+1 - skip index
                        filled_fields = ['RA', 'DEC', 'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e5', 'ID_e1234']
                        if field not in filled_fields:
                            setattr(meta_object, field, row[i+1])  # Similar to source.field = row[i+1]

                    # sources.append(source)
                    meta_object.save()
                    # link sources with created meta object
                    Command.link_source_with_meta(meta_object, row)
                    # find master_source and take name, survey, RA, DEC, EXT, R98, LIKE from it
                    Command.find_master_source(meta_object)
                    # find or create meta group for created meta object
                    if not meta_object.meta_group:
                        Command.find_or_create_meta_group(meta_object)

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: meta_object.delete()
                    raise CommandError(e)

            # rename and copy images TODO: image names
            Command.rename_copy_images(row.img_id, meta_object.meta_ind)

            # maybe use this later
            # if len(sources) > 500:
            #     Source.objects.bulk_create(sources)
            #     sources = []
            #

        # if sources:
        #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading table')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading Parquet took: {(end_time-start_time).total_seconds()} seconds.'))