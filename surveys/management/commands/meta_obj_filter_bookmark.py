import argparse

from django.core.management import BaseCommand, CommandError
from django.contrib.auth.models import User

from surveys.models import MetaObjFilterBookmark


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
                                 "Must be valid GET request.")
        parser.add_argument("-d", "--description", type=str,
                            help="Filter description "
                                 "(optional).")
        parser.add_argument("--overwrite", action="store_true")
        parser.add_argument("--delete", action="store_true")

    def handle(self, *args, **options):
        if options["delete"]:
            print("Delete custom sources filter from homepage.")
        elif options["overwrite"]:
            print("Edit custom sources filter on homepage.")
        else:
            print("Create new custom sources filter for homepage.")

        name = options["name"] or input("Enter filter name: ")
        if not name:
            raise CommandError("Name can not be empty")

        try:
            sources_filter = MetaObjFilterBookmark.objects.get(name=name)

            if not options["overwrite"] and not options["delete"]:
                raise CommandError(f"Filter already exists:\n{sources_filter}")

            if options["delete"]:
                sources_filter.delete()
                return

        except MetaObjFilterBookmark.DoesNotExist:
            if options["delete"]:
                raise CommandError(f"No filter named {name}")

            sources_filter = MetaObjFilterBookmark(name=name)

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
                "Enter filtering criteria (must be valid GET request): ")
        )

        description = options["description"]
        if description is None:
            description = input(
                "Filter description (optional, may be empty): "
            )

        sources_filter.author = author
        sources_filter.criteria = MetaObjFilterBookmark.parse_url(criteria)
        sources_filter.description = description

        if not get_user_agreement(
                f"Please check input:\n{sources_filter}\nAdd filter?"):
            print("Aborting...")
            return

        sources_filter.save()
        print("Done.")
