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
    help = "Convert master data from PKL to Parquet file."

    @staticmethod
    def get_fields():  # add img_id to identify images in load_data
        fields = ['img_id', 'RA', 'DEC', 'unchange_flag', 'comment', 'object_class', 'EXT', 'R98', 'LIKE',
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
    def get_table_schema():
        fields = [
            pa.field('img_id', pa.int64()),  # to identify Pavel images
            pa.field("RA", pa.float64(), False),
            pa.field("DEC", pa.float64(), False),
            pa.field('unchange_flag', pa.bool_()),
            pa.field("comment", pa.string()),
            pa.field("object_class", pa.string()),

            # master table fields
            pa.field('EXT', pa.float64()),
            pa.field('R98', pa.float64()),
            pa.field('LIKE', pa.float64()),

            pa.field('D2D_e1m', pa.float64()),
            pa.field('D2D_e2m', pa.float64()),
            pa.field('D2D_e3m', pa.float64()),
            pa.field('D2D_e4m', pa.float64()),
            pa.field('D2D_me1', pa.float64()),
            pa.field('D2D_me2', pa.float64()),
            pa.field('D2D_me3', pa.float64()),
            pa.field('D2D_me4', pa.float64()),

            pa.field('EXP_e1', pa.float64()),
            pa.field('EXP_e2', pa.float64()),
            pa.field('EXP_e3', pa.float64()),
            pa.field('EXP_e4', pa.float64()),
            pa.field('EXP_e1234', pa.float64()),

            pa.field('ID_FLAG_e1m', pa.int64()),
            pa.field('ID_FLAG_e2m', pa.int64()),
            pa.field('ID_FLAG_e3m', pa.int64()),
            pa.field('ID_FLAG_e4m', pa.int64()),
            pa.field('ID_FLAG_me1', pa.int64()),
            pa.field('ID_FLAG_me2', pa.int64()),
            pa.field('ID_FLAG_me3', pa.int64()),
            pa.field('ID_FLAG_me4', pa.int64()),

            pa.field('ID_e1', pa.int64()),
            pa.field('ID_e2', pa.int64()),
            pa.field('ID_e3', pa.int64()),
            pa.field('ID_e4', pa.int64()),
            pa.field('ID_e1234', pa.int64()),

            pa.field('RATIO_e2e1', pa.float64()),
            pa.field('RATIO_e3e2', pa.float64()),
            pa.field('RATIO_e4e3', pa.float64()),

            pa.field('RFLAG_e2e1', pa.int64()),
            pa.field('RFLAG_e3e2', pa.int64()),
            pa.field('RFLAG_e4e3', pa.int64()),

            pa.field('R_NSRC_e1m', pa.int64()),
            pa.field('R_NSRC_e2m', pa.int64()),
            pa.field('R_NSRC_e3m', pa.int64()),
            pa.field('R_NSRC_e4m', pa.int64()),
            pa.field('R_NSRC_me1', pa.int64()),
            pa.field('R_NSRC_me2', pa.int64()),
            pa.field('R_NSRC_me3', pa.int64()),
            pa.field('R_NSRC_me4', pa.int64()),

            pa.field('UPLIM_e1', pa.float64()),
            pa.field('UPLIM_e2', pa.float64()),
            pa.field('UPLIM_e3', pa.float64()),
            pa.field('UPLIM_e4', pa.float64()),
            pa.field('UPLIM_e1234', pa.float64()),

            pa.field('flag', pa.int64()),

            pa.field('TSTART_e1', pa.string()),
            pa.field('TSTART_e2', pa.string()),
            pa.field('TSTART_e3', pa.string()),
            pa.field('TSTART_e4', pa.string()),

            pa.field('TSTOP_e1', pa.string()),
            pa.field('TSTOP_e2', pa.string()),
            pa.field('TSTOP_e3', pa.string()),
            pa.field('TSTOP_e4', pa.string()),
            # end of master table fields
        ]
        schema = pa.schema(fields)

        return schema

    def handle(self, *args, **options):
        start_time = timezone.now()
        file_path = os.path.join(settings.MASTER_DIR, 'emaster_43691_48110_03_23_sd01_a15_g12_r7.pkl')
        with open(file_path, 'rb') as f:
            data = pickle.load(f)

        master_sources = data

        # TODO: change this later (object, datetime64[ns] -> string)
        for col in master_sources.columns:
            if master_sources[col].dtype == np.object:
                print("Column {} type: {}".format(col, master_sources[col].dtype))
                master_sources[col] = master_sources[col].astype(str)
            if is_datetime(master_sources[col]):
                print("Column {} type: {}".format(col, master_sources[col].dtype))
                master_sources[col] = pd.to_datetime(master_sources[col]).dt.date
                master_sources[col] = master_sources[col].astype(str)

        master_sources['img_id'] = master_sources.index
        master_sources.reset_index(drop=True, inplace=True)

        # Check if all field names in dataframe - add them
        fields = Command.get_fields()
        for field in fields:
            if field not in master_sources.columns:
                master_sources[field] = None

        master_sources = master_sources[fields]
        print(master_sources)

        # Save parquet table with specified schema
        schema = Command.get_table_schema()
        table = pa.Table.from_pandas(master_sources, schema=schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, 'master_sources.parquet'))

        self.stdout.write(f'End converting pkl')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Converting PKL took: {(end_time - start_time).total_seconds()} seconds.'))