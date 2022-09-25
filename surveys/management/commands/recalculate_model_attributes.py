import argparse

from django.core.management import BaseCommand
from django.db.models import Model

import surveys.models as sm


class Command(BaseCommand):
    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "models", metavar="MODELS", type=str, nargs="+",
            help="List of models to recalculate attributes for."
        )

    def handle(self, *args, **options):
        for model_name in options["models"]:
            try:
                model: Model = getattr(sm, model_name)
            except AttributeError:
                print(f"Model {model_name} not found.")
                continue

            for obj in model.objects.all():
                obj.save()
