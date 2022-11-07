from argparse import ArgumentParser
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from django.core.management import BaseCommand, CommandError
from django.db import connection as db_connection
import django.db.models as models

import surveys.models as sm


class Command(BaseCommand):
    """A command to export anonimous data. I.e. without coordinates, external
    ids. For joining tables, an internal, anonimous ids will be provided.
    """

    include_attributes = {
        sm.MetaObject: [
            "id", "master_survey", "object_class", "g_s", "ls_g_s",
            "flag_agn_wise", "tde_v3", "tde_v3_ls",
            "EXT", "R98", "LIKE", "primary_object", "meta_group"
        ],
        sm.eROSITA: [
            "id", "survey_ind", "source_class",

            "g_s", "ls_g_s", "flag_agn_wise",  # автоматическая классификация

            "ML_FLUX_0", "ML_FLUX_ERR_0", "flux_05_20",
            "EXT", "EXT_LIKE", "EXT_ERR",

            # id (внутренние) оптических источников
            "ls_dup", "ls_dup_sep", "sdss_dup", "sdss_dup_sep",
            "ps_dup", "ps_dup_sep", "gaia_dup", "gaia_dup_sep",
        ],
        sm.LS: [
            "id", "opt_id", "flag_agn_wise", "star",

            "ebv",  "pmra", "pmdec", "parallax", "pmra_ivar", "pmdec_ivar",
            "parallax_ivar",

            "flux_g", "flux_ivar_g", "flux_r", "flux_ivar_r", "flux_z",
            "flux_ivar_z", "flux_w1", "flux_ivar_w1",
            "flux_w2", "flux_ivar_w2", "flux_w3", "flux_ivar_w3",
            "flux_w4", "flux_ivar_w4",

            "flux_g_ebv", "flux_r_ebv", "flux_z_ebv", "flux_w1_ebv",
            "flux_w2_ebv", "flux_w3_ebv", "flux_w4_ebv",

            "mag_r_ab", "mag_err_r_ab", "mag_g_ab", "mag_err_g_ab", "mag_z_ab",
            "mag_err_z_ab", "mag_w1_ab", "mag_err_w1_ab", "mag_w2_ab",
            "mag_err_w2_ab", "mag_w3_ab", "mag_err_w3_ab", "mag_w4_ab",
            "mag_err_w4_ab",

            # "xray_sources",  # ???
        ],
        sm.PS: [
            "id", "opt_id", "qualityFlag", "primaryDetection",

            "w1fit", "w1bad", "w1mag", "dw1mag", "w2fit", "w2bad", "w2mag",
            "dw2mag",

            "gPSFFlux", "gPSFFluxErr", "rPSFFlux", "rPSFFluxErr", "iPSFFlux",
            "iPSFFluxErr", "zPSFFlux", "zPSFFluxErr", "yPSFFlux",
            "yPSFFluxErr",

            "gKronFlux", "gKronFluxErr", "rKronFlux", "rKronFluxErr",
            "iKronFlux", "iKronFluxErr", "zKronFlux", "zKronFluxErr",
            "yKronFlux", "yKronFluxErr",

            "gKronMagAB", "gKronMagErrAB", "rKronMagAB", "rKronMagErrAB",
            "iKronMagAB", "iKronMagErrAB", "zKronMagAB", "zKronMagErrAB",
            "yKronMagAB", "yKronMagErrAB",

            # "xray_sources",  # ???
        ],
        sm.SDSS: [
            "id", "opt_id",

            "cModelFlux_u", "cModelFluxIvar_u", "cModelFlux_g",
            "cModelFluxIvar_g", "cModelFlux_r", "cModelFluxIvar_r",
            "cModelFlux_i", "cModelFluxIvar_i", "cModelFlux_z",
            "cModelFluxIvar_z",

            "psfFlux_u", "psfFluxIvar_u", "psfFlux_g", "psfFluxIvar_g",
            "psfFlux_r", "psfFluxIvar_r", "psfFlux_i", "psfFluxIvar_i",
            "psfFlux_z", "psfFluxIvar_z",

            "cModelMag_u_ab", "cModelMagErr_u_ab", "cModelMag_g_ab",
            "cModelMagErr_g_ab", "cModelMag_r_ab", "cModelMagErr_r_ab",
            "cModelMag_i_ab", "cModelMagErr_i_ab", "cModelMag_z_ab",
            "cModelMagErr_z_ab",

            # "xray_sources",  # ???
        ],
        sm.GAIA: [
            "id", "opt_id", "star",

            "parallax", "parallax_error",
            "pm", "pmra", "pmra_error", "pmdec", "pmdec_error",
            "autoclass_star",

            # "xray_sources",  # ???
        ],
    }

    many_to_many_relation_tables = [
        "surveys_erosita_meta_objects",
        "surveys_ls_xray_sources",
        "surveys_ps_xray_sources",
        "surveys_sdss_xray_sources",
        "surveys_ps_xray_sources",
    ]

    def add_arguments(self, parser: ArgumentParser):
        parser.add_argument("-o", "--output", type=Path, required=True,
                            help="An output directory.")

    @staticmethod
    def replace_none(data, model):
        """Replaces None-s with specified values for Pandas to infer types
        correctly.

        :param data: .values() from Django model. I.e. list of dictionaries
        :param model: corresponding Django model
        """
        columns = list(data[0].keys())
        default_values = dict()
        for column_name in columns:
            column_type: models.Field = getattr(model, column_name).field

            if (isinstance(column_type, models.FloatField)
                    or isinstance(column_type, models.DecimalField)):
                default_values[column_name] = np.nan
                continue

            if (isinstance(column_type, models.IntegerField)
                    or isinstance(column_type, models.PositiveIntegerField)
                    or isinstance(column_type, models.BigAutoField)):
                default_values[column_name] = -999
                continue

            if isinstance(column_type, models.BooleanField):
                default_values[column_name] = -999
                continue

            if isinstance(column_type, models.CharField):
                default_values[column_name] = ""
                continue

            if isinstance(column_type, models.ForeignKey):
                default_values[column_name] = -999
                continue

            raise CommandError(
                f"Unsupported type at column {column_type}: "
                f"{type(column_type)}"
            )

        result = list()
        for line in data:
            for column_name, value in line.items():
                if value is None:
                    line[column_name] = default_values[column_name]

            result.append(line)

        return result

    def handle(self, *args, **options):
        output_directory: Path = options["output"]

        if not output_directory.exists():
            os.makedirs(output_directory)

        for model, attributes in self.include_attributes.items():
            data = model.objects.all().values(*attributes)
            self.replace_none(data, model)
            data = pd.DataFrame.from_records(data)  # .replace(np.nan, "NaN")

            filename = f"surveys_{model.__name__.lower()}.parquet"
            data.to_parquet(output_directory / filename, engine='fastparquet',
                            compression="GZIP")

        for table in self.many_to_many_relation_tables:
            with db_connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            data = list(dict(zip(columns, values)) for values in data)
            data = pd.DataFrame.from_records(data)

            filename = f"{table}.parquet"
            data.to_parquet(output_directory / filename, engine="fastparquet",
                            compression="GZIP")
