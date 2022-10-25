from argparse import ArgumentParser
from pathlib import Path

from astropy.table import Table
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
        parser.add_argument("--column_rename_prefix", type=str, default="",
                            help="Prefix to add to colums names for "
                                 "compatibility with something other. idk)")

    @timeit("Exporting coordinates")
    def handle(self, *args, **options):
        catalog_model_name = options["catalog"]
        ra_column = options["ra"]
        dec_column = options["dec"]
        additional_columns = options["additional_columns"] or []
        column_rename_prefix = options["column_rename_prefix"]

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

        required_columns = [model._meta.pk.name, ra_column, dec_column]
        required_columns += additional_columns
        renamed_columns = {column: column_rename_prefix + column
                           for column in required_columns}

        coordinates_df = model.objects.all().values(*required_columns)
        coordinates_df = (
            pd.DataFrame.from_records(coordinates_df).rename(renamed_columns)
        )

        save_format = options["format"]
        dst_path = options["output"]

        if save_format == "gz_pkl":
            coordinates_df.to_pickle(dst_path, compression="gzip", protocol=4)
            return

        if save_format == "fits":
            Table.from_pandas(coordinates_df).write(dst_path, overwrite=True)
            return

        raise CommandError("Only gz_pkl and fits formats are supported")
