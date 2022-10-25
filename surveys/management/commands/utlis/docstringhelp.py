"""Sets "help" attribute for Django Command to be docstring."""
import textwrap
from typing import Type, Callable

from django.core.management import BaseCommand


def docstringhelp(cls: Type[BaseCommand]) -> Type[BaseCommand]:
    cls.help = "" if cls.__doc__ is None else textwrap.dedent(cls.__doc__)
    return cls
