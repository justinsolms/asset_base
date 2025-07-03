"""Provides a command-line interface for the package.

The CLI provides the following commands:
- init: Initialize a fresh database and populate it with required data.

"""
# Immediately suppress numexpr < warning level logs so the autocomplete string
# outputs work form the command line interface.
import logging
logging.getLogger("numexpr").setLevel(logging.WARNING)

import click
from asset_base.manager import Manager

# Get module-named logger.
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """
    Tool for managing the financial database and updating it with fresh data
    from the API.
    """
    pass

@click.command()
@click.option("-d", "--delete-dump-data", is_flag=True, help="Delete dump data.")
def init(delete_dump_data):
    """Initialize a fresh database and populate it with required data."""
    with Manager() as abm:
        if delete_dump_data:
            logger.warning("Deleting dump data.")
            abm.tear_down(delete_dump_data=True)
        else:
            logger.info("Retaining dump data.")
            abm.tear_down(delete_dump_data=False)
        abm.set_up()

cli.add_command(init)

if __name__ == "__main__":
    cli()
