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


class Command(BaseCommand):
    help = "Load data from Parquet file."

    def add_arguments(self, parser):
        parser.add_argument('survey_num', type=int, help='number of survey')

    @staticmethod
    def get_fields():
        fields = ['survey_ind', 'name', 'RA', 'DEC', 'comment', 'source_class', 'GLON', 'GLAT',
                    'pos_r98', 'DET_LIKE_0', 'ML_FLUX_0', 'ML_FLUX_ERR_0', 'ML_CTS_0', 'ML_CTS_ERR_0',
                    'ML_EXP_1', 'EXT', 'EXT_LIKE', 'EXT_ERR', 'hpidx', 'RADEC_ERR', 'ML_BKG_0', 'ML_RATE_0',
                    'ML_RATE_ERR_0', 'tilenum', 'ID_SRC', 'ID_CLUSTER', 'DIST_NN', 'SRCDENS', 'NH',
                    'RA_corr', 'DEC_corr', 'astro_indx', 'astro_nx', 'astro_mdra',
                    'astro_mddec', 'astro_fit_nx', 'astro_fit_sigma', 'astro_fit_ro_opt',
                    'astro_flag', 'pos_sigma_2d', 'pos_r68', 'pos_r95',
                    'ELON', 'ELAT', 'flux_05_20', 'nH_pow', 'nH_pow_l', 'nH_pow_u',
                    'nH_pow_st', 'G_pow', 'G_pow_l', 'G_pow_u', 'G_pow_st', 'norm_pow',
                    'norm_pow_l', 'norm_pow_u', 'norm_pow_st', 'c_pow', 'dof_pow', 'nH_dbb',
                    'nH_dbb_l', 'nH_dbb_u', 'nH_dbb_st', 'kT_dbb', 'kT_dbb_l', 'kT_dbb_u',
                    'kT_dbb_st', 'norm_dbb', 'norm_dbb_l', 'norm_dbb_u', 'norm_dbb_st',
                    'c_dbb', 'dof_dbb', 'TSTART', 'TSTOP', 'survey', 'file_name']
        return fields

    def handle(self, *args, **options):
        start_time = timezone.now()
        # get number of loading survey
        survey_num = options['survey_num']
        load_file_name = 'xray_sources_' + str(survey_num) + '.parquet'
        file_path = os.path.join(settings.WORK_DIR, load_file_name)

        # Field list in the order in which the columns should be in the table
        field_list = Command.get_fields()

        table = pq.read_table(file_path)
        data = table.to_pandas()
        # replace all nan with None
        data = data.replace({np.nan: None})

        self.stdout.write(f'Start reading data')
        # sources = []
        print(data)

        for row in data.itertuples():
            # find/create meta source - file
            origin_file, f_created = OriginFile.objects.get_or_create(file_name=row.file_name)
            if f_created:
                self.stdout.write(f'Create new file object for: {row.file_name}')

            # find source's survey
            try:
                survey = Survey.objects.get(name=row.survey)
            except Survey.DoesNotExist:
                raise CommandError(f'Survey{row.survey} not found')

            # TODO: use hpidx instead of survey_ind?
            source, created = eROSITA.objects.get_or_create(survey_ind=row.survey_ind, name=row.name,
                                                            survey=survey, origin_file=origin_file,
                                                            defaults={'RA': row.RA, 'DEC': row.DEC})
            if created:
                self.stdout.write(f'{row[0]} - Create new source {row.survey_ind} with name: {row.name}, survey: {row.survey}')

            # Check that it is new source or new file
            if f_created or created:
                try:
                    self.stdout.write(f'Start filling fields...\n')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+1]}')  # i+1 - skip index
                        filled_fields = ['survey_ind', 'name', 'survey', 'file_name', 'RA', 'DEC']
                        if field not in filled_fields:
                            setattr(source, field, row[i+1])  # Similar to source.field = row[i+1]

                    # sources.append(source)
                    source.save()

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: source.delete()
                    raise CommandError(e)

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
