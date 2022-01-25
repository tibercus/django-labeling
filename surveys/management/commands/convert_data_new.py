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
        fields = ['img_id', 'name', 'RA', 'DEC', 'ztf_name', 'comment', 'source_class', 'master_source', 'dup_id', 'L', 'B',
                  'R98', 'FLAG', 'qual','g_d2d', 'g_s', 'g_id', 's_d2d', 's_id', 's_z', 's_otype', 's_nsrc', 'checked',
                  'flag_xray','flag_radio', 'flag_agn_wise', 'w1', 'w2', 'w3', 'w1snr', 'w2snr', 'w3snr',
                  'g_nsrc', 'sdss_nsrc', 'sdss_p', 'sdss_id', 'sdss_sp', 'sdss_d2d', 'added',  '_15R98', 'g_gmag',
                  'g_maxLx', 'w_nsrc', 'R98c', 'z', 'CTS_e1', 'CTS_e2', 'CTS_e3', 'CTS_e4', 'CTS_e123',
                  'D2D_e1', 'D2D_e3e2', 'D2D_e4', 'D2D_e123',
                  'EXP_e1', 'EXP_e12', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e123',
                  'FLUX_e1', 'FLUX_e2', 'FLUX_e3', 'FLUX_e4', 'FLUX_e123',
                  'G_L_e1', 'G_L_e2', 'G_L_e3', 'G_U_e1', 'G_U_e2', 'G_U_e3', 'G_e1', 'G_e2', 'G_e3',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e123',
                  'LIKE_e1', 'LIKE_e12', 'LIKE_e2', 'LIKE_e3', 'LIKE_e4', 'LIKE_e123',
                  'NH_L_e1', 'NH_L_e2', 'NH_L_e3', 'NH_U_e1', 'NH_U_e2', 'NH_U_e3', 'NH_e1', 'NH_e2', 'NH_e3',
                  'RADEC_ERR_e1', 'RADEC_ERR_e2', 'RADEC_ERR_e3', 'RADEC_ERR_e4', 'RADEC_ERR_e12', 'RADEC_ERR_e123',
                  'Tin_L_e1', 'Tin_L_e2', 'Tin_L_e3', 'Tin_U_e1', 'Tin_U_e2', 'Tin_U_e3', 'Tin_e1', 'Tin_e2', 'Tin_e3',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e12', 'UPLIM_e123', 'RATIO_e3e2',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4',
                  'g_b', 'ps_p', 'survey', 'file', 'row_num']
        return fields

    @staticmethod
    def get_table_schema():
        fields = [
            pa.field("img_id", pa.int64()),  # to identify Pavel images
            pa.field("name", pa.string(), False),
            pa.field("RA", pa.float64(), False),
            pa.field("DEC", pa.float64(), False),
            pa.field("ztf_name", pa.string()),
            pa.field("comment", pa.string()),
            pa.field("source_class", pa.string()),
            pa.field("master_source", pa.bool_()),
            pa.field("dup_id", pa.int64()),
            # master table fields
            pa.field("L", pa.float64()),
            pa.field("B", pa.float64()),
            pa.field("R98", pa.float64()),
            pa.field("FLAG", pa.int64()),
            pa.field("qual", pa.int64()),
            pa.field("g_d2d", pa.float64()),
            pa.field("g_s", pa.int64()),
            pa.field("g_id", pa.int64()),
            pa.field("s_d2d", pa.float64()),
            pa.field("s_id", pa.string()),
            pa.field("s_z", pa.float64()),
            pa.field("s_otype", pa.string()),
            pa.field("s_nsrc", pa.int64()),
            pa.field("checked", pa.int64()),
            # flags
            pa.field("flag_xray", pa.int64()),
            pa.field("flag_radio", pa.int64()),
            pa.field("flag_agn_wise", pa.int64()),
            # w1,2,3
            pa.field("w1", pa.float64()),
            pa.field("w2", pa.float64()),
            pa.field("w3", pa.float64()),
            # w1snr,2,3
            pa.field("w1snr", pa.float64()),
            pa.field("w2snr", pa.float64()),
            pa.field("w3snr", pa.float64()),

            pa.field("g_nsrc", pa.int64()),
            # sdss
            pa.field("sdss_nsrc", pa.int64()),
            pa.field("sdss_p", pa.int64()),
            pa.field("sdss_id", pa.int64()),
            pa.field("sdss_sp", pa.int64()),
            pa.field("sdss_d2d", pa.float64()),

            pa.field("added", pa.string()),
            pa.field("_15R98", pa.float64()),
            pa.field("g_gmag", pa.float64()),
            pa.field("g_maxLx", pa.float64()),
            pa.field("w_nsrc", pa.int64()),
            pa.field("R98c", pa.float64()),
            pa.field("z", pa.float64()),
            # CTS_e1,2,3,4
            pa.field("CTS_e1", pa.float64()),
            pa.field("CTS_e2", pa.float64()),
            pa.field("CTS_e3", pa.float64()),
            pa.field("CTS_e4", pa.float64()),
            pa.field("CTS_e123", pa.float64()),
            # D2D
            pa.field("D2D_e1", pa.float64()),
            pa.field("D2D_e3e2", pa.float64()),
            pa.field("D2D_e4", pa.float64()),
            pa.field("D2D_e123", pa.float64()),
            # EXP_e1,2,3,4
            pa.field("EXP_e1", pa.float64()),
            pa.field("EXP_e2", pa.float64()),
            pa.field("EXP_e3", pa.float64()),
            pa.field("EXP_e4", pa.float64()),
            pa.field("EXP_e12", pa.float64()),
            pa.field("EXP_e123", pa.float64()),
            # FLUX_e1,2,3,4
            pa.field("FLUX_e1", pa.float64()),
            pa.field("FLUX_e2", pa.float64()),
            pa.field("FLUX_e3", pa.float64()),
            pa.field("FLUX_e4", pa.float64()),
            pa.field("FLUX_e123", pa.float64()),
            # G_..._e1,2,3
            pa.field("G_L_e1", pa.float64()),
            pa.field("G_L_e2", pa.float64()),
            pa.field("G_L_e3", pa.float64()),
            pa.field("G_U_e1", pa.float64()),
            pa.field("G_U_e2", pa.float64()),
            pa.field("G_U_e3", pa.float64()),
            pa.field("G_e1", pa.float64()),
            pa.field("G_e2", pa.float64()),
            pa.field("G_e3", pa.float64()),
            # ID_e1,2,3,4
            pa.field("ID_e1", pa.int64()),
            pa.field("ID_e2", pa.int64()),
            pa.field("ID_e3", pa.int64()),
            pa.field("ID_e4", pa.int64()),
            pa.field("ID_e123", pa.int64()),
            # LIKE_e1,2,3,4
            pa.field("LIKE_e1", pa.float64()),
            pa.field("LIKE_e2", pa.float64()),
            pa.field("LIKE_e3", pa.float64()),
            pa.field("LIKE_e4", pa.float64()),
            pa.field("LIKE_e12", pa.float64()),
            pa.field("LIKE_e123", pa.float64()),
            # NH_..._e1,2,3
            pa.field("NH_L_e1", pa.float64()),
            pa.field("NH_L_e2", pa.float64()),
            pa.field("NH_L_e3", pa.float64()),
            pa.field("NH_U_e1", pa.float64()),
            pa.field("NH_U_e2", pa.float64()),
            pa.field("NH_U_e3", pa.float64()),
            pa.field("NH_e1", pa.float64()),
            pa.field("NH_e2", pa.float64()),
            pa.field("NH_e3", pa.float64()),
            # RADEC_ERR_e1,2,3,4
            pa.field("RADEC_ERR_e1", pa.float64()),
            pa.field("RADEC_ERR_e2", pa.float64()),
            pa.field("RADEC_ERR_e3", pa.float64()),
            pa.field("RADEC_ERR_e4", pa.float64()),
            pa.field("RADEC_ERR_e12", pa.float64()),
            pa.field("RADEC_ERR_e123", pa.float64()),

            # Tin_..._e1,2,3
            pa.field("Tin_L_e1", pa.float64()),
            pa.field("Tin_L_e2", pa.float64()),
            pa.field("Tin_L_e3", pa.float64()),
            pa.field("Tin_U_e1", pa.float64()),
            pa.field("Tin_U_e2", pa.float64()),
            pa.field("Tin_U_e3", pa.float64()),
            pa.field("Tin_e1", pa.float64()),
            pa.field("Tin_e2", pa.float64()),
            pa.field("Tin_e3", pa.float64()),
            # UPLIM_e1,2,3,4
            pa.field("UPLIM_e1", pa.float64()),
            pa.field("UPLIM_e2", pa.float64()),
            pa.field("UPLIM_e3", pa.float64()),
            pa.field("UPLIM_e4", pa.float64()),
            pa.field("UPLIM_e12", pa.float64()),
            pa.field("UPLIM_e123", pa.float64()),

            pa.field("RATIO_e3e2", pa.float64()),
            # TSTART_e1,2,3,4
            pa.field("TSTART_e1", pa.string()),
            pa.field("TSTART_e2", pa.string()),
            pa.field("TSTART_e3", pa.string()),
            pa.field("TSTART_e4", pa.string()),
            # TSTOP_e1,2,3,4
            pa.field("TSTOP_e1", pa.string()),
            pa.field("TSTOP_e2", pa.string()),
            pa.field("TSTOP_e3", pa.string()),
            pa.field("TSTOP_e4", pa.string()),

            pa.field("g_b", pa.int64()),
            pa.field("ps_p", pa.int64()),
            # end of master table fields
            pa.field("survey", pa.int64()),
            pa.field("file", pa.string()),
            pa.field("row_num", pa.int64()),
        ]
        schema = pa.schema(fields)

        return schema

    def handle(self, *args, **options):
        # self.stdout.write(f'Pandas version: {pd.__version__}')
        start_time = timezone.now()
        file_path = os.path.join(settings.MASTER_DIR, 'uniq_master_45930_47023_03_22_18.pkl')
        # file_path = os.path.join(settings.MASTER_DIR, 'test_dup_id.pkl')
        with open(file_path, 'rb') as f:
            data = pickle.load(f)

        master_xray_sources = data.rename(columns={'SRCNAME': 'name', 'ZTF': 'ztf_name',
                                                   'ID': 'source_class', 'notes': 'comment', '15R98': '_15R98'})
        # TODO: change this later (object, datetime64[ns] -> string)
        for col in master_xray_sources.columns:
            if master_xray_sources[col].dtype == np.object:
                print("Column {} type: {}".format(col, master_xray_sources[col].dtype))
                master_xray_sources[col] = master_xray_sources[col].astype(str)
            if is_datetime(master_xray_sources[col]) or 'TSTART' in col or 'TSTOP' in col:
                print("Column {} type: {}".format(col, master_xray_sources[col].dtype))
                master_xray_sources[col] = pd.to_datetime(master_xray_sources[col]).dt.date
                master_xray_sources[col] = master_xray_sources[col].astype(str)

        # master_xray_sources['master_source'] = True
        master_xray_sources['row_num'] = range(len(master_xray_sources.index))
        master_xray_sources['img_id'] = master_xray_sources.index
        # TODO: remove this later (set survey)
        master_xray_sources.reset_index(drop=True, inplace=True)
        master_xray_sources['survey'] = 1
        master_xray_sources[10:25]['survey'] = 2
        master_xray_sources[25:40]['survey'] = 3
        master_xray_sources[40:]['survey'] = 4

        # Write to column 'file' name of converted file without '/'
        # start = file_path.rfind('\\') if os.name == 'nt' else file_path.rfind('/')
        start = file_path.rfind('/')
        master_xray_sources['file'] = file_path[start+1:-4]

        # Check if all field names in dataframe - add them
        fields = Command.get_fields()
        for field in fields:
            if field not in master_xray_sources.columns:
                master_xray_sources[field] = None

        master_xray_sources = master_xray_sources[fields]
        # print(master_xray_sources)

        # Save parquet table with specified schema
        schema = Command.get_table_schema()
        table = pa.Table.from_pandas(master_xray_sources, schema=schema)
        pq.write_table(table, os.path.join(settings.WORK_DIR, 'master_xray_sources.parquet'))
        # pq.write_table(table, os.path.join(settings.WORK_DIR, 'test_dup_id.parquet'))

        self.stdout.write(f'End converting pkl')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Converting PKL took: {(end_time - start_time).total_seconds()} seconds.'))