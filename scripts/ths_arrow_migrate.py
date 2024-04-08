"""
A cli script to read rows from THS (LOCAL SQLITE ) and write them out using pyarrow.
"""

# flake8: noqa
"""
Console script for querying tables before and after import/migration to ensure that we have what we expect
"""

# import importlib
# import itertools
# import json
import logging
import os
import pathlib
import random

import click

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
# logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('toshi_hazard_store').setLevel(logging.WARNING)

from nzshm_common.grids import load_grid
from nzshm_common.location.code_location import CodedLocation
from pynamodb.models import Model

import toshi_hazard_store  # noqa: E402
import toshi_hazard_store.config
import toshi_hazard_store.query.hazard_query
# from scripts.core import echo_settings  # noqa
# from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
# from toshi_hazard_store.config import USE_SQLITE_ADAPTER  # noqa
# from toshi_hazard_store.config import LOCAL_CACHE_FOLDER
# from toshi_hazard_store.config import REGION as THS_REGION
# from toshi_hazard_store.db_adapter.dynamic_base_class import ensure_class_bases_begin_with, set_base_class
# from toshi_hazard_store.db_adapter.sqlite import (  # noqa this is needed to finish the randon-rlz functionality
#     SqliteAdapter,
# )

nz1_grid = load_grid('NZ_0_1_NB_1_1')
#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|


@click.command()
@click.option('-o', '--output_folder', default=lambda: os.getcwd(), help="defaults to Current Working Directory")
@click.option(
    '--source',
    '-S',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL",
)
@click.option(
    '--how-many',
    '-n',
    type=int,
    default=1,
    help="set the source store. defaults to LOCAL",
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
def main(output_folder, source, how_many, verbose, dry_run):
    """migrate from SOURCE to output folder"""
    nz1_grid = load_grid('NZ_0_1_NB_1_1')
    locs=[CodedLocation(o[0], o[1], 0.001).code for o in random.sample(nz1_grid, how_many)]
    click.echo(f'count {how_many}: {locs}')
