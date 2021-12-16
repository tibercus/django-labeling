from django.core.management import BaseCommand
from django.utils import timezone
import pandas as pd
import pickle
import pyarrow as pa
import pyarrow.parquet as pq

from django.conf import settings
import os

# from surveys.models import *


class Command(BaseCommand):
    help = "Convert data from PKL to Parquet file."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for pkl file')

    @staticmethod
    def get_fields():
        fields = ['name', 'RA', 'DEC', 'ztf_name', 'comment', 'source_class', 'master_source', 'dup_id', 'L', 'B',
                  'R98', 'g_d2d', 'g_s', 'g_nsrc', 'g_gmag', 's_d2d', 's_id', 's_z', 's_otype', 'flag_agn_wise',
                  'flag_xray', 'flag_radio', 'sdss_p', 'sdss_nsrc', 'RATIO_e2e1', 'FLUX_e1', 'FLUX_e2', 'FLUX_e3',
                  'CTS_e1', 'CTS_e2', 'CTS_e3', 'EXP_e1', 'EXP_e2', 'EXP_e3', 'LIKE_e1', 'LIKE_e2', 'LIKE_e3', 'G_L_e2',
                  'G_e2', 'G_U_e2', 'Tin_L_e2', 'Tin_e2', 'Tin_U_e2', 'NH_L_e2', 'NH_e2', 'NH_U_e2', 'UPLIM_e1',
                  'UPLIM_e2', 'UPLIM_e3', 'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3',
                  'survey', 'file', 'row_num']
        return fields

    @staticmethod
    def get_table_schema():
        fields = [
            pa.field("name", pa.string(), False),
            pa.field("RA", pa.float64(), False),
            pa.field("DEC", pa.float64(), False),
            pa.field("ztf_name", pa.string()),
            pa.field("comment", pa.string()),
            pa.field("source_class", pa.string()),
            pa.field("master_source", pa.bool_()),
            pa.field("dup_id", pa.int64()),
            pa.field("L", pa.float64()),
            pa.field("B", pa.float64()),
            pa.field("R98", pa.float64()),
            pa.field("g_d2d", pa.float64()),
            pa.field("g_s", pa.float64()),
            pa.field("g_nsrc", pa.float64()),
            pa.field("g_gmag", pa.float64()),
            pa.field("s_d2d", pa.float64()),
            pa.field("s_id", pa.float64()),
            pa.field("s_z", pa.float64()),
            pa.field("s_otype", pa.float64()),
            pa.field("flag_agn_wise", pa.float64()),
            pa.field("flag_xray", pa.float64()),
            pa.field("flag_radio", pa.float64()),
            pa.field("sdss_p", pa.float64()),
            pa.field("sdss_nsrc", pa.float64()),
            pa.field("RATIO_e2e1", pa.float64()),
            pa.field("FLUX_e1", pa.float64()),
            pa.field("FLUX_e2", pa.float64()),
            pa.field("FLUX_e3", pa.float64()),
            pa.field("CTS_e1", pa.float64()),
            pa.field("CTS_e2", pa.float64()),
            pa.field("CTS_e3", pa.float64()),
            pa.field("EXP_e1", pa.float64()),
            pa.field("EXP_e2", pa.float64()),
            pa.field("EXP_e3", pa.float64()),
            pa.field("LIKE_e1", pa.float64()),
            pa.field("LIKE_e2", pa.float64()),
            pa.field("LIKE_e3", pa.float64()),
            pa.field("G_L_e2", pa.float64()),
            pa.field("G_e2", pa.float64()),
            pa.field("G_U_e2", pa.float64()),
            pa.field("Tin_L_e2", pa.float64()),
            pa.field("Tin_e2", pa.float64()),
            pa.field("Tin_U_e2", pa.float64()),
            pa.field("NH_L_e2", pa.float64()),
            pa.field("NH_e2", pa.float64()),
            pa.field("NH_U_e2", pa.float64()),
            pa.field("UPLIM_e1", pa.float64()),
            pa.field("UPLIM_e2", pa.float64()),
            pa.field("UPLIM_e3", pa.float64()),
            pa.field("TSTART_e1", pa.string()),
            pa.field("TSTART_e2", pa.string()),
            pa.field("TSTART_e3", pa.string()),
            pa.field("TSTOP_e1", pa.string()),
            pa.field("TSTOP_e2", pa.string()),
            pa.field("TSTOP_e3", pa.string()),
            pa.field("survey", pa.int64()),
            pa.field("file", pa.string()),
            pa.field("row_num", pa.int64()),
        ]
        schema = pa.schema(fields)

        return schema

    def handle(self, *args, **options):
        # self.stdout.write(f'Pandas version: {pd.__version__}')
        start_time = timezone.now()
        # file_path = options["file_path"]
        file_path = os.path.join(settings.PAVEL_DIR, 'pavel_file.pkl')
        with open(file_path, 'rb') as f:
            data = pickle.load(f)

        xray_sources = data.rename(columns={'SRCNAME': 'name', 'ZTF': 'ztf_name', 'ID': 'source_class',
                                            'notes': 'comment'
                                            })
        # TODO: change this later
        xray_sources['dup_id'] = xray_sources.index
        xray_sources['master_source'] = True
        xray_sources['row_num'] = range(len(xray_sources.index))

        xray_sources.reset_index(drop=True, inplace=True)
        xray_sources.drop(columns=['w1', 'w2', 'w3', 'w1snr', 'w2snr', 'w3snr', 'w_nsrc',
                                   'FLUX_e4', 'CTS_e4', 'EXP_e4', 'LIKE_e4', 'UPLIM_e4',
                                   'ID_e2', 'ID_e3', 'ID_e4', 'TSTART_e4', 'TSTOP_e4', 'added'], inplace=True)
        xray_sources['survey'] = 1
        xray_sources[4:8]['survey'] = 2
        xray_sources[8:]['survey'] = 3
        # Write to column 'file' name of converted file without '/'
        start = file_path.rfind('\\') if os.name == 'nt' else file_path.rfind('/')
        xray_sources['file'] = file_path[start+1:-4]

        # Check if all field names in dataframe - add them
        fields = Command.get_fields()
        for field in fields:
            if field not in xray_sources.columns:
                xray_sources[field] = None

        # Save parquet table with specified schema
        schema = Command.get_table_schema()
        table = pa.Table.from_pandas(xray_sources, schema=schema)
        pq.write_table(table, 'surveys/test_xray_data/xray_sources.parquet')

        xray_sources = xray_sources[fields]
        xray_sources.to_parquet('surveys/test_xray_data/xray_sources.parquet', engine='fastparquet')

        self.stdout.write(f'End converting pkl')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Converting PKL took: {(end_time - start_time).total_seconds()} seconds.'))