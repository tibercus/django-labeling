import argparse
import textwrap

from django.core.management import BaseCommand, CommandError
from django.db.utils import IntegrityError

from surveys.models import MetaObjectClass
from .calc_mags import BaseCommandWithFormattedHelp
from surveys.utils import help_from_docstring


@help_from_docstring
class Command(BaseCommandWithFormattedHelp):
    """Manage meta object classes.

    Actions:
        --add Adds a class,
        --delete Deletes a class,
        --init-default Initialises database table with default classes
            (TDE, AGN, GALAXY, Other, Unknown).

    Class attributes:
        --id Class identifier,
        --name Class human-readable name,
        --desc Optional class description.

    Examples:
        > python manage.py meta_object_class --add --id <class_id> \\
            --name <class_name> [--desc <class_description>]
        > python manage.py meta_object_class --delete --id <class_id>
        > python manage.py meta_object_class --init-default
    """

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("--add", action="store_true", help="Add class.")
        parser.add_argument("--delete", action="store_true",
                            help="Delete class.")
        parser.add_argument("--init-default", action="store_true",
                            help="Init default classes "
                                 "(TDE, AGN, Galaxy, Other, Unknown).")

        parser.add_argument("--id", type=str, default=None,
                            help="Id of a class.")
        parser.add_argument("--name", type=str, default=None,
                            help="Human-readable class name. Required when"
                                 "using --add")
        parser.add_argument("--desc", type=str, default=None,
                            help="Description of a class. Optional.")

    @staticmethod
    def validate_arguments(*args, **options):
        actions_chosen = (
            options["add"] + options["delete"] + options["init_default"]
        )
        if actions_chosen != 1:
            raise CommandError("So, --add or --delete or --init-default?")

        if options["init_default"]:
            return

        id_not_specified = options["id"] is None
        name_not_specified = options["name"] is None

        if options["add"] and (id_not_specified or name_not_specified):
            raise CommandError("Both --id and --name must be specified to "
                               "add a meta object class.")

        if options["delete"] and id_not_specified:
            raise CommandError("--id must be specified to delete "
                               "a meta object class")

    def handle(self, *args, **options):
        self.validate_arguments(*args, **options)

        if options["add"]:
            try:
                MetaObjectClass.objects.create(
                    id=options["id"],
                    name=options["name"],
                    desc=options["desc"],
                )
            except IntegrityError:
                raise CommandError(f"Class with --id {options['id']}"
                                   f"already exists.")

            return

        if options["delete"]:
            try:
                MetaObjectClass.objects.get(id=options["id"]).delete()
            except MetaObjectClass.DoesNotExist:
                raise CommandError(f"Class with --id {options['id']} "
                                   f"does not exist.")

            return

        if options["init_default"]:
            default_classes = [
                ('TDE', 'Class TDE'),
                ('AGN', 'Class AGN'),
                ('Galactic', 'Class Galactic'),
                ('Other', 'Other Class'),
                ('Unk', 'Unknown'),
            ]
            for class_id, class_name in default_classes:
                try:
                    MetaObjectClass.objects.create(
                        id=class_id, name=class_name, desc=None
                    )
                    print(f"Class id={class_id}, name={class_name} created.")
                except IntegrityError:
                    print(f"Class id={class_id}, name={class_name} "
                          f"already exists. Skipping.")

            return
