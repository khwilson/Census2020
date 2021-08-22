from pathlib import Path
from typing import Optional

import click
import pyarrow.parquet as pq
import us
from tqdm import tqdm

from . import downloader


@click.group()
def cli():
    """ Interacting with 2020 PL94 data """


@cli.command("pull")
@click.argument("state")
@click.argument("output")
def pull_command(state: str, output: str):
    table = downloader.get_state(state)
    pq.write_table(table, output)


@cli.command("pull-all")
@click.option("--output", "-o", type=click.Path(file_okay=False, dir_okay=True))
def pull_all_command(output: Optional[str] = None):
    output_dir = Path(output or ".")

    output_dir.mkdir(exist_ok=True, parents=True)
    for state in tqdm(set(us.STATES) | {us.states.DC}):
        table = downloader.get_state(state.abbr)
        pq.write_table(table, output_dir / f"{state.abbr.lower()}.parquet")


if __name__ == "__main__":
    cli()
