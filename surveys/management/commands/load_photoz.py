from argparse import ArgumentParser
import itertools
import logging
import os
from pathlib import Path
import re
from typing import Type

import pandas as pd
from django.core.management import CommandError
from django.db import models

from surveys.models import LS, PS, PhotoZPrediction
from .utlis import BaseCommandWithFormattedHelp
from .utlis import timeit
from .utlis import docstringhelp


@docstringhelp
class Command(BaseCommandWithFormattedHelp):
    """A command to load photo-z predictions from pzph1dot1.py script on hea134

    Predictions must contain foreign keys to LS and/or PS objects to define
    connection between prediction itself and optical objects. Prediction must
    contain at least one FK.

    Predictions must be present in form as pzph1dot1 gives them.

    Hint: --input argument should be the same as --outputDirectory of pzph1dot1
    """
    updated_counter = 0

    def add_arguments(self, parser: ArgumentParser):
        parser.add_argument("-i", "--input", type=Path, required=True,
                            help="A path to directory with predictions.")
        parser.add_argument("--ps_id", type=str, required=False,
                            help="A column with PS FK.")
        parser.add_argument("--ls_id", type=str, required=False,
                            help="A column with LS FK.")
        parser.add_argument("-m", "--model", type=str, required=True,
                            help="A model to load predictions from. E.g."
                                 "'best-x1a', 'x1a22', etc.")

    @staticmethod
    def validate_arguments(*args, **options):
        if not (options["ps_id"] or options["ls_id"]):
            raise CommandError("Either --ls_id or --ps_id must be specified.")

        if not (os.path.isdir(path := options["input"])):
            raise CommandError(
                f"Invalid path to predictions directory: {path}")

    @staticmethod
    def model_series_from_model_name(_name: str, /) -> str:
        """Parse model names like 'best-x1a' or 'x1a35' to model series."""
        parsed_model_name = re.findall(
            r"^(\w.*?)(\d{2})$|^(best-)(.*)$", _name)[0]

        model_name = parsed_model_name[0] or parsed_model_name[3]
        if model_name is None or len(model_name) == 0:
            raise IndexError(f"Empty model name from {_name}.")

        return model_name

    @staticmethod
    def read_predictions_part(path: Path, chunk_number: int,
                              model_series: str) -> pd.DataFrame:
        """A helper function to read a chunk of predictions (read and join
        features-file, predictions-file and best-file) from specified
        directory.
        """
        filename_template = "part-{chunk:05d}.{filetype}.gz_pkl"

        features_file = pd.read_pickle(
            path / filename_template.format(
                chunk=chunk_number, filetype="features"),
            compression="gzip"
        )

        predictions_file = pd.read_pickle(
            path / filename_template.format(
                chunk=chunk_number, filetype=f"predictions.{model_series}"),
            compression="gzip"
        )
        best_file = pd.read_pickle(
            path / filename_template.format(
                chunk=chunk_number, filetype=f"best.{model_series}"),
            compression="gzip"
        )
        return pd.concat([features_file, predictions_file, best_file], axis=1)

    def load_predictions(self, data: pd.DataFrame,
                         fk_columns: dict[Type[models.Model], str],
                         model_name: str):
        """Line by line load predictions and create FK-connections."""
        for _, row in data.iterrows():
            if pd.isna(row[f"zoo_{model_name}_z_max"]):
                continue

            try:
                ps_object_id = row[fk_columns[PS]]
            except (KeyError, PS.DoesNotExist):
                ps_object_id = None

            try:
                ls_object_id = row[fk_columns[LS]]
            except (KeyError, PS.DoesNotExist):
                ls_object_id = None

            prediction_object, _ = (
                PhotoZPrediction.objects
                .get_or_create(ps_object_id=ps_object_id,
                               ls_object_id=ls_object_id,
                               model_name=model_name)
            )

            prediction_object.z_max = row[f"zoo_{model_name}_z_max"]
            prediction_object.z_conf = row[f"zoo_{model_name}_z_maxConf"]
            prediction_object.ci_68_a = row[f"zoo_{model_name}_ci1a_68"]
            prediction_object.ci_68_b = row[f"zoo_{model_name}_ci1b_68"]
            prediction_object.ci_95_a = row[f"zoo_{model_name}_ci1a_95"]
            prediction_object.ci_95_b = row[f"zoo_{model_name}_ci1b_95"]

            prediction_object.save()
            self.updated_counter += 1

    @timeit("Read and load predictions")
    def handle(self, *args, **options):
        """Reads predictions part-by-part from specified input directory and
        loads them into PhotoZPrediction.
        """
        self.validate_arguments(*args, **options)
        model_name = options["model"]
        input_path = options["input"]

        # mapping model -> id column in predictions
        foreign_keys_columns: dict[Type[models.Model], str] = dict()

        if column := options["ls_id"]:
            foreign_keys_columns[LS] = column

        if column := options["ps_id"]:
            foreign_keys_columns[PS] = column

        try:
            model_series = self.model_series_from_model_name(model_name)
        except IndexError:
            raise CommandError(f"Invalid model is specified: {model_name}")

        for chunk in itertools.count(start=0, step=1):
            try:
                chunk_data = self.read_predictions_part(
                    input_path, chunk, model_series)
            except FileNotFoundError:
                break

            print(chunk_data["zoo_best-x1a_z_max"].notna().sum())
            self.load_predictions(chunk_data, foreign_keys_columns, model_name)

        logging.info(f"Loaded/updated {self.updated_counter} records.")
