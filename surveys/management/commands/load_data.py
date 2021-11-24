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
        skip_list = ['id', 'comments', 'opt_sources']
        # Field list in the order in which the columns should be in the table.csv
        field_list = [field.name for field in Source._meta.get_fields() if field.name not in skip_list]

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

                self.stdout.write(f'Source name: {row[0]} RA: {row[1]} DEC: {row[2]}')
                source, create = Source.objects.get_or_create(name=row[0], RA=row[1], DEC=row[2], survey=survey)
                if create:
                    self.stdout.write(f'Create new source with name:{row[0]}, RA:{row[1]}, DEC{row[2]}')

                self.stdout.write(f'Start filling fields')
                for i, field in enumerate(field_list):
                    # self.stdout.write(f'Num:{i} - {field}')
                    if field not in ['name', 'RA', 'DEC', 'survey']:
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
