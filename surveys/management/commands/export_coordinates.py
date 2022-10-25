from argparse import ArgumentParser
from pathlib import Path

from django.core.management import CommandError
import pandas as pd

import surveys.models as smd
from .utlis import timeit
from .utlis import BaseCommandWithFormattedHelp
from .utlis import docstringhelp


@docstringhelp
class Command(BaseCommandWithFormattedHelp):

    def add_arguments(self, parser: ArgumentParser):
        parser.add_argument("-o", "--output", type=Path, required=True)
        parser.add_argument("--catalog", type=str, required=True,
                            help="Django model name, that represents catalog. "
                                 "Case-sensitive.")
        parser.add_argument("--ra", type=str, required=True,
                            help="Column name with Right Ascension.")
        parser.add_argument("--dec", type=str, required=True,
                            help="Column name with Declination.")
        parser.add_argument("--format", type=str, default="gz_pkl")
        parser.add_argument("--additional_columns", type=str, nargs="*")

    @timeit("Exporting coordinates")
    def handle(self, *args, **options):
        catalog_model_name = options["catalog"]
        ra_column = options["ra"]
        dec_column = options["dec"]
        additional_columns = options["additional_columns"] or []

        try:
            model = getattr(smd, catalog_model_name)
        except AttributeError:
            raise CommandError(
                f"No Django model named '{catalog_model_name}' found.")

        invalid_columns = [
            column for column in [ra_column, dec_column] + additional_columns
            if not hasattr(model, column)
        ]

        if invalid_columns:
            raise CommandError(f"Those columns are not present in {model}: "
                               f"{', '.join(invalid_columns)}.")

        coordinates_df = model.objects.all().values(
            model._meta.pk.name, ra_column, dec_column, *additional_columns
        )

        coordinates_df = pd.DataFrame.from_records(coordinates_df)

        if options["format"] != "gz_pkl":
            raise CommandError(
                "Formats other than 'gz_pkl' are not supported yet.")

        coordinates_df.to_pickle(options["output"],
                                 compression="gzip", protocol=4)
