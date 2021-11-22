import csv
from django.core.management import BaseCommand
from django.utils import timezone

from surveys.models import *


class Command(BaseCommand):
    help = "Loads data from CSV file."

    def add_arguments(self, parser):
        parser.add_argument("file_path", type=str, help='path for csv file')

    def handle(self, *args, **options):
        start_time = timezone.now()
        file_path = options["file_path"]
        # Field list in the order in which the columns should be in the table.csv
        field_list = ['name', 'RA', 'DEC', 'ztf_name', 'comment', 'source_class', 'master_source', 'dup_id', 'L', 'B',
                      'R98', 'g_d2d', 'g_s', 'g_nsrc', 'g_gmag', 's_d2d', 's_id', 's_z', 's_otype', 'w_e1', 'w_e2','w_e3',
                      'w_snr_e1', 'w_snr_e2', 'w_snr_e3', 'w_nsrc', 'flag_agn_wise', 'flag_xray', 'flag_radio',
                      'sdss_p', 'sdss_nsrc', 'RATIO_e2e1', 'FLUX_e1', 'FLUX_e2', 'FLUX_e3', 'CTS_e1', 'CTS_e2', 'CTS_e3',
                      'EXP_e1', 'EXP_e2', 'EXP_e3', 'LIKE_e1', 'LIKE_e2', 'LIKE_e3', 'G_L_e2', 'G_e2', 'G_U_e2', 'Tin_L_e2',
                      'Tin_e2', 'Tin_U_e2', 'NH_L_e2', 'NH_e2', 'NH_U_e2', 'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3',
                      'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'survey']

        skip_list = ['name', 'RA', 'DEC', 'comments', 'opt_sources', 'survey']
        with open(file_path, "r") as csv_file:
            data = csv.reader(csv_file, delimiter=",")
            next(data)  # to skip header of table
            self.stdout.write(f'Start reading csv')
            # sources = []
            for row in data:
                try:
                    survey = Survey.objects.get(name=row[-1])
                except Survey.DoesNotExist:
                    self.stdout.write((self.style.ERROR(f'Survey{row[-1]} not found')))

                # self.stdout.write(f'Source name: {row[0]} RA: {row[1]} DEC: {row[2]}')
                source, create = Source.objects.get_or_create(name=row[0], RA=row[1], DEC=row[2], survey=survey)
                if create:
                    self.stdout.write(f'Create new source with name:{row[0]}, RA:{row[1]}, DEC{row[2]}')

                self.stdout.write(f'Start filling fields')
                for i, field in enumerate(field_list):
                    # self.stdout.write(f'Num:{i} - {field}')
                    if field not in skip_list:
                        value = row[i] if row[i] else None
                        setattr(source, field, value)  # Similar to source.field = row[i]

                # sources.append(source)
                source.save()

            #     if len(sources) > 500:
            #         Source.objects.bulk_create(sources)
            #         sources = []
            #
            # if sources:
            #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading csv')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading CSV took: {(end_time-start_time).total_seconds()} seconds.'))
