"""Provides a command-line interface for the package.

The CLI provides the following commands:
- init: Initialize a fresh database and populate it with required data.

"""
import logging

from asset_base.exceptions import NotSetUp

import click
from asset_base.manager import Manager

from importlib.metadata import version

# Get module-named logger.
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version("asset-base"), '-v', '--version', message='version==%(version)s')
def cli():
    """
    Tool for managing the financial database and updating it with fresh data
    from the API.
    """
    pass

@click.command()
@click.option(
    "--hard", is_flag=True,
    help="Hard initialization, delete reusable dump data. Not recommended unless data is stale.")
def setup(hard):
    """Tear down old database, dumping reusable data, initialize a new one, and
    populate it with data. Reuse dumped data to expedite population of the new
    database.

    WARNING: Dump data may be stale especially security meta-data. Use
    the --hard option to delete dump data and force a full fresh population of
    the new database. This is not recommended unless absolutely necessary as it
    is time-consuming."""
    with Manager() as abm:
        if hard:
            hard = True
        else:
            hard = False
        abm.tear_down(delete_dump_data=hard)
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
