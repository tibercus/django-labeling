import argparse

from django.core.management import BaseCommand, CommandError
from django.contrib.auth.models import User

from surveys.models import CustomSourcesFilter, MetaObject


def get_user_agreement(message: str, /) -> bool:
    return input(f"{message} [y/N]: ").upper() == 'Y'


class Command(BaseCommand):
    """A command to add custom filters."""

    help = __doc__

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("-n", "--name", type=str, help="Filter name.")
        parser.add_argument("-a", "--author", type=str,
                            help="Author (valid username).")
        parser.add_argument("-c", "--criteria", type=str,
                            help="Filtering criteria. "
                                 "Must be valid SQL filter.")
        parser.add_argument("-d", "--description", type=str,
                            help="Filter description "
                                 "(optional).")
        parser.add_argument("--overwrite", action="store_true")

    @staticmethod
    def check_user(username: str):
        pass  # TODO

    @staticmethod
    def check_sql_filter(condition: str):
        """Check that filter is valid and is not dummy
        (does not return all objects)
        """
        pass  # TODO

    def handle(self, *args, **options):
        print("Setup new custom sources filter for homepage.")

        name = options["name"] or input("Enter filter name: ")
        if not name:
            raise CommandError("Name can not be empty")

        author = (
            options["author"]
            or input("Enter author (valid username): ")
        )
        if not author:
            raise CommandError("Author can not be empty")

        try:
            author = User.objects.get(username=author)
        except User.DoesNotExist as dne:
            print(dne)
            return

        criteria = (
            options["criteria"]
            or input(
                "Enter filtering criteria (Must be valid SQL filter): ")
        )

        description = options["description"]
        if description is None:
            description = input(
                "Filter description (optional, may be empty): "
            )

        try:
            sources_filter = CustomSourcesFilter.objects.get(name=name)
            print(sources_filter)
            if not options["overwrite"]:
                raise CommandError(f"Filter already exists:\n{sources_filter}")

        except CustomSourcesFilter.DoesNotExist:
            sources_filter = CustomSourcesFilter(name=name)

        sources_filter.author = author
        sources_filter.criteria = criteria
        sources_filter.description = description

        if not get_user_agreement(
                f"Please check input:\n{sources_filter}\nAdd filter?"):
            print("Aborting...")
            return


        # TODO check if filter exists

        sources_filter.save()
        print("Done.")
