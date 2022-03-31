from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime  # for datetime64[ns] format
import numpy as np

import pickle
import pyarrow as pa
import pyarrow.parquet as pq

from django.conf import settings
import os

# from surveys.models import *


class Command(BaseCommand):
    help = "Convert eROSITA sources from PKL to Parquet file."

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

    @staticmethod
    def get_table_schema():
        fields = [
            pa.field('survey_ind', pa.int64()),  # to link with master table
            pa.field('name', pa.string(), False),
            pa.field('RA', pa.float64(), False),
            pa.field('DEC', pa.float64(), False),
            pa.field('comment', pa.string()),
            pa.field('source_class', pa.string()),
            # pa.field('master_source', pa.bool_()),

            # eROSITA table fields
            pa.field('GLON', pa.float64()),
            pa.field('GLAT', pa.float64()),
            pa.field('pos_r98', pa.float64()),
            pa.field('DET_LIKE_0', pa.float64()),

            pa.field('ML_FLUX_0', pa.float64()),
            pa.field('ML_FLUX_ERR_0', pa.float64()),
            pa.field('ML_CTS_0', pa.float64()),
            pa.field('ML_CTS_ERR_0', pa.float64()),
            pa.field('ML_EXP_1', pa.float64()),

            pa.field('EXT', pa.float64()),
            pa.field('EXT_LIKE', pa.float64()),
            pa.field('EXT_ERR', pa.float64()),

            pa.field('hpidx', pa.int64()),
            pa.field('RADEC_ERR', pa.float64()),

            pa.field('ML_BKG_0', pa.float64()),
            pa.field('ML_RATE_0', pa.float64()),
            pa.field('ML_RATE_ERR_0', pa.float64()),

            pa.field('tilenum', pa.int64()),
            pa.field('ID_SRC', pa.int64()),
            pa.field('ID_CLUSTER', pa.int64()),
            pa.field('DIST_NN', pa.float64()),
            pa.field('SRCDENS', pa.float64()),
            pa.field('NH', pa.float64()),
            pa.field('RA_corr', pa.float64()),
            pa.field('DEC_corr', pa.float64()),

            pa.field('astro_indx', pa.int64()),
            pa.field('astro_nx', pa.float64()),
            pa.field('astro_mdra', pa.float64()),
            pa.field('astro_mddec', pa.float64()),
            pa.field('astro_fit_nx', pa.float64()),
            pa.field('astro_fit_sigma', pa.float64()),
            pa.field('astro_fit_ro_opt', pa.float64()),
            pa.field('astro_flag', pa.int64()),

            pa.field('pos_sigma_2d', pa.float64()),
            pa.field('pos_r68', pa.float64()),
            pa.field('pos_r95', pa.float64()),

            pa.field('ELON', pa.float64()),
            pa.field('ELAT', pa.float64()),
            pa.field('flux_05_20', pa.float64()),

            pa.field('nH_pow', pa.float64()),
            pa.field('nH_pow_l', pa.float64()),
            pa.field('nH_pow_u', pa.float64()),
            pa.field('nH_pow_st', pa.float64()),

            pa.field('G_pow', pa.float64()),
            pa.field('G_pow_l', pa.float64()),
            pa.field('G_pow_u', pa.float64()),
            pa.field('G_pow_st', pa.int64()),

            pa.field('norm_pow', pa.float64()),
            pa.field('norm_pow_l', pa.float64()),
            pa.field('norm_pow_u', pa.float64()),
            pa.field('norm_pow_st', pa.int64()),

            pa.field('c_pow', pa.float64()),
            pa.field('dof_pow', pa.int64()),

            pa.field('nH_dbb', pa.float64()),
            pa.field('nH_dbb_l', pa.float64()),
            pa.field('nH_dbb_u', pa.float64()),
            pa.field('nH_dbb_st', pa.int64()),

            pa.field('kT_dbb', pa.float64()),
            pa.field('kT_dbb_l', pa.float64()),
            pa.field('kT_dbb_u', pa.float64()),
            pa.field('kT_dbb_st', pa.int64()),

            pa.field('norm_dbb', pa.float64()),
            pa.field('norm_dbb_l', pa.float64()),
            pa.field('norm_dbb_u', pa.float64()),
            pa.field('norm_dbb_st', pa.int64()),

            pa.field('c_dbb', pa.float64()),
            pa.field('dof_dbb', pa.int64()),

            pa.field('TSTART', pa.string()),
            pa.field('TSTOP', pa.string()),
            # end of eROSITA table

            pa.field('survey', pa.int64()),
            pa.field('file_name', pa.string()),
        ]
        schema = pa.schema(fields)

        return schema

    @staticmethod
    def get_file_paths(survey_num):
        # get file with ls data and ps data by survey number
        if survey_num == 1:
            file_path = os.path.join(settings.MASTER_DIR, 'ecat_43691_44775_03_23_sd01_a15_g15_r7.pkl')
        elif survey_num == 2:
            file_path = os.path.join(settings.MASTER_DIR, 'ecat_44775_45897_03_23_sd01_a15_g15_r7.pkl')
        elif survey_num == 3:
            file_path = os.path.join(settings.MASTER_DIR, 'ecat_45897_46992_03_23_sd01_a15_g15_r7.pkl')
        elif survey_num == 4:
            file_path = os.path.join(settings.MASTER_DIR, 'ecat_46992_48110_03_23_sd01_a15_g15_r7.pkl')
        elif survey_num == 9:
            file_path = os.path.join(settings.MASTER_DIR, 'ecat_43691_48110_03_23_sd01_a15_g14_r7.pkl')

        return file_path

    def handle(self, *args, **options):
        start_time = timezone.now()
        # get number of loading survey
        survey_num = options['survey_num']
        # get file path by survey number
        file_path = Command.get_file_paths(survey_num)

        with open(file_path, 'rb') as f:
            data = pickle.load(f)

        xray_sources = data.drop(columns=['RA', 'DEC', 'srcname', 'RADEC_ERR'])
        xray_sources = xray_sources.rename(columns={'srcname_fin': 'name', 'RA_fin': 'RA', 'DEC_fin': 'DEC',
                                                    'RADEC_ERR_fin': 'RADEC_ERR', 'flux_05-20': 'flux_05_20'})

        # TODO: change this later (object, datetime64[ns] -> string)
        for col in xray_sources.columns:
            if xray_sources[col].dtype == np.object:
                print("Column {} type: {}".format(col, xray_sources[col].dtype))
                xray_sources[col] = xray_sources[col].astype(str)
            if is_datetime(xray_sources[col]):
                print("Column {} type: {}".format(col, xray_sources[col].dtype))
                xray_sources[col] = pd.to_datetime(xray_sources[col]).dt.date
                xray_sources[col] = xray_sources[col].astype(str)

        xray_sources['survey_ind'] = xray_sources.index
        xray_sources.reset_index(drop=True, inplace=True)
        xray_sources['survey'] = survey_num

        file_name = os.path.splitext(os.path.basename(file_path))[0]
        xray_sources['file_name'] = file_name

        # Check if all field names in dataframe - add them
        fields = Command.get_fields()
        for field in fields:
            if field not in xray_sources.columns:
                xray_sources[field] = None

        xray_sources = xray_sources[fields]
        print(xray_sources)

        # Save parquet table with specified schema
        schema = Command.get_table_schema()
        table = pa.Table.from_pandas(xray_sources, schema=schema)
        convert_file_name = 'xray_sources_' + str(survey_num) + '.parquet'
        pq.write_table(table, os.path.join(settings.WORK_DIR, convert_file_name))

        self.stdout.write(f'End converting pkl')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Converting PKL took: {(end_time - start_time).total_seconds()} seconds.'))
