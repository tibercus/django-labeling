import csv
from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd

from surveys.models import *


class Command(BaseCommand):
    help = "Loads data from Parquet file."

    def add_arguments(self, parser):
        parser.add_argument("file_path", type=str, help='path for csv file')

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

    def handle(self, *args, **options):
        start_time = timezone.now()
        file_path = options["file_path"]
        # Field list in the order in which the columns should be in the table
        field_list = Command.get_fields()

        data = pd.read_parquet(file_path, engine='fastparquet')
        self.stdout.write(f'Start reading data')
        # sources = []
        for row in data.itertuples():
            meta_data, m_created = MetaSource.objects.get_or_create(file_name=row.file)
            if m_created:
                self.stdout.write(f'Create new meta source for file: {row.file}')

            try:
                survey = Survey.objects.get(name=row.survey)
            except Survey.DoesNotExist:
                raise CommandError(f'Survey{row.survey} not found')

            self.stdout.write(f'Source name: {row.name} RA: {row.RA} DEC: {row.DEC}, survey: {row.survey}')
            # TODO: think about RA, DEC - dont work because it's float
            source, created = Source.objects.get_or_create(name=row.name, survey=survey,
                                                           defaults={'RA': row.RA, 'DEC': row.DEC,
                                                                     'meta_data': meta_data, 'row_num': row.row_num})
            if created:
                self.stdout.write(f'Create new source with name:{row.name}, RA: {row.RA}, '
                                  f'DEC: {row.DEC}, file:{row.file}')

            # Check that it is new source or new file
            if m_created or created:
                try:
                    self.stdout.write(f'Start filling fields')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+1]}')
                        if field not in ['name', 'RA', 'DEC', 'survey', 'file']:
                            # value = row[i+1] if row[i+1] else None
                            setattr(source, field, row[i+1])  # Similar to source.field = row[i+1]

                    # sources.append(source)
                    source.save()
                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: source.delete()
                    raise CommandError(e)

            # if len(sources) > 500:
            #     Source.objects.bulk_create(sources)
            #     sources = []
            #
            # if sources:
            #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading table')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading Parquet took: {(end_time-start_time).total_seconds()} seconds.'))
