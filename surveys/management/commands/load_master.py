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


class Command(BaseCommand):
    help = "Loads data from Parquet file."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for parquet file')

    @staticmethod
    def get_fields():  # add img_id to identify images in load_data
        fields = ['RA', 'DEC', 'unchange_flag', 'comment', 'object_class', 'EXT', 'R98', 'LIKE',
                  'D2D_e1m', 'D2D_e2m', 'D2D_e3m', 'D2D_e4m', 'D2D_me1', 'D2D_me2', 'D2D_me3', 'D2D_me4',
                  'EXP_e1', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e1234',
                  'ID_FLAG_e1m', 'ID_FLAG_e2m', 'ID_FLAG_e3m', 'ID_FLAG_e4m',
                  'ID_FLAG_me1', 'ID_FLAG_me2', 'ID_FLAG_me3', 'ID_FLAG_me4',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e1234',
                  'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3', 'RFLAG_e2e1', 'RFLAG_e3e2', 'RFLAG_e4e3',
                  'R_NSRC_e1m', 'R_NSRC_e2m', 'R_NSRC_e3m', 'R_NSRC_e4m',
                  'R_NSRC_me1', 'R_NSRC_me2', 'R_NSRC_me3', 'R_NSRC_me4',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e1234', 'flag',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4']
        return fields

    @staticmethod
    def rename_copy_images(img_id, meta_id):
        # Rename and Copy images to dir: static/images
        # img_id, master_name - attributes of master table

        images_path = settings.MASTER_DIR
        for i in range(1, 10):
            # Copy Light Curve
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_lc' + '.pdf')
            if os.path.isfile(old_path):
                new_file_name = 'lc_' + str(meta_id) + '.pdf'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

            # Copy Spectrum
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_spec' + '.pdf')
            if os.path.isfile(old_path):
                new_file_name = 'spec_' + str(meta_id) + '.pdf'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

            # Copy Trans Image
            old_path = os.path.join(images_path, 'eRASS'+str(i), 'src_' + str(img_id) + '_e' + str(i) + '.png')
            if os.path.isfile(old_path):
                new_file_name = 'trans_' + str(meta_id) + '.png'
                new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                shutil.copy(old_path, new_path)

    @staticmethod
    def link_source_with_meta(meta_object, row_values):
        if row_values.ID_e1 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e1, survey=Survey.objects.get(name=1))
                source.meta_objects.add(meta_object)
                print(f'Add meta object: {row_values.img_id} - {meta_object} to source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e1} from survey 1 not found')

        if row_values.ID_e2 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e2, survey=Survey.objects.get(name=2))
                source.meta_objects.add(meta_object)
                print(f'Add meta object: {row_values.img_id} - {meta_object} to source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e2} from survey 2 not found')

        if row_values.ID_e3 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e3, survey=Survey.objects.get(name=3))
                source.meta_objects.add(meta_object)
                print(f'Add meta object: {row_values.img_id} - {meta_object} to source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e3} from survey 3 not found')

        if row_values.ID_e4 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e4, survey=Survey.objects.get(name=4))
                source.meta_objects.add(meta_object)
                print(f'Add meta object: {row_values.img_id} - {meta_object} to source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e4} from survey 4 not found')

        if row_values.ID_e1234 > 0:
            try:
                source = eROSITA.objects.get(survey_ind=row_values.ID_e1234, survey=Survey.objects.get(name=9))
                source.meta_objects.add(meta_object)
                print(f'Add meta object: {row_values.img_id} - {meta_object} to source: {source.survey_ind} - {source}')
                source.save()
            except eROSITA.DoesNotExist:
                raise CommandError(f'Source with survey_ind: {row_values.ID_e1234} from survey 9 not found')

    @staticmethod
    def find_master_source(meta_object):
        sources = meta_object.object_sources.all()
        if sources:
            max_dl0 = sources.aggregate(Max('DET_LIKE_0'))['DET_LIKE_0__max']
            master_source = sources.get(DET_LIKE_0=max_dl0)
            print(f'Found master_source  - {master_source.name} with DET_LIKE_0: {max_dl0}')
            # take name, survey, RA, DEC, EXT, R98, LIKE from master_source
            meta_object.master_name = master_source.name
            meta_object.master_survey = master_source.survey.name
            # TODO: uncomment later
            # meta_object.RA = master_source.RA
            # meta_object.DEC = master_source.DEC
            # meta_object.EXT = master_source.EXT
            # meta_object.R98 = master_source.pos_r98
            # meta_object.LIKE = master_source.DET_LIKE_0
            meta_object.save()

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
            meta_object, created = MetaObject.objects.get_or_create(ID_e1=row.ID_e1, ID_e2=row.ID_e2,
                                                                    ID_e3=row.ID_e3, ID_e4=row.ID_e4,
                                                                    defaults={'RA': row.RA, 'DEC': row.DEC})

            # Check that it is new meta object
            if created:
                self.stdout.write(f'Create new meta object with img_id: {row.img_id}, RA: {row.RA}, DEC: {row.DEC}')
                try:
                    self.stdout.write(f'Start filling fields...\n')
                    for i, field in enumerate(field_list):
                        self.stdout.write(f'Num:{i} - {field} - {row[i+2]}')  # i+2 - skip index and img_id
                        filled_fields = ['RA', 'DEC', 'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4']
                        if field not in filled_fields:
                            setattr(meta_object, field, row[i+2])  # Similar to source.field = row[i+1]

                    # sources.append(source)
                    meta_object.save()
                    # link sources with created meta object
                    Command.link_source_with_meta(meta_object, row)
                    # find master_source and take name, survey, RA, DEC, EXT, R98, LIKE from it
                    Command.find_master_source(meta_object)
                    # TODO: think about image names
                    Command.rename_copy_images(row.img_id, meta_object.pk)

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: meta_object.delete()
                    raise CommandError(e)

            # Rename and Copy images to static/images
            # Command.rename_copy_images(row.img_id, row.row_num, row.file)

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