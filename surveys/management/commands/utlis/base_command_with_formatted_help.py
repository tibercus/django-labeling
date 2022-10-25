"""A simple child of BaseCommand to handle help as a docstring"""
import argparse
from abc import ABC

from django.core.management import BaseCommand


class BaseCommandWithFormattedHelp(BaseCommand, ABC):
    def create_parser(self, *args, **kwargs):
        parser = super().create_parser(*args, **kwargs)
        parser.formatter_class = argparse.RawTextHelpFormatter
        return parser
