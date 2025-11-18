"""Provides a command-line interface for the package.

The CLI provides the following commands:
- init: Initialize a fresh database and populate it with required data.

"""
import logging

from asset_base.exceptions import NotSetUp

import click
from asset_base.manager import Manager

import pkg_resources

# Get module-named logger.
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(pkg_resources.get_distribution("asset-base").version, '-v', '--version', message='version==%(version)s')
def cli():
    """
    Tool for managing the financial database and updating it with fresh data
    from the API.
    """
    pass

@click.command()
@click.option(
    "-d", "--delete-dump-data", is_flag=True,
    help="Delete reusable dump data (not recommended).")
def setup(delete_dump_data):
    """Tear down old database, dumping reusable data, set up a new one, and
    populate it with data. Reuse dumped data to expedite population of the new
    database."""
    with Manager() as abm:
        if delete_dump_data:
            delete_dump_data = True
        else:
            delete_dump_data = False
        abm.tear_down(delete_dump_data=delete_dump_data)
        abm.set_up()

@click.command()
def update():
    """Update the database with fresh data from the API."""
    with Manager() as abm:
        try:
            abm.update()
        except NotSetUp as ex:
            logger.error(
                "The database has not been set up. Please run the 'init' "
                "command first.")

@click.command()
def delete():
    """Delete (drop) the entire database and all its contents."""
    # Prompt the user for confirmation before deleting the database.
    click.confirm(
        "Are you sure you want to delete the database? This action cannot be "
        "undone.", abort=True)

    # If the user confirms, proceed with deleting the database.
    with Manager() as abm:
        try:
            abm.close(drop=True)
        except NotSetUp as ex:
            logger.error(
                "The database has not been set up. Please run the 'init' "
                "command first.")
        else:
            logger.info("Database successfully deleted.")


@click.command()
def dump():
    """Dump the database reusable data for later use in setting up a new
    database."""
    with Manager() as abm:
        try:
            abm.dump()
        except NotSetUp as ex:
            logger.error(
                "The database has not been set up. Please run the 'init' "
                "command first.")
        else:
            logger.info("Database reusable data dumped successfully for use in "
                        "a new database setup.")

cli.add_command(setup)
cli.add_command(update)
cli.add_command(delete)
cli.add_command(dump)

if __name__ == "__main__":
    cli()
