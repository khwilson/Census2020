import tempfile
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import pyarrow as pa
import pyarrow.compute as pc
import requests
import us

from census2020.parsers import (
    combine_tables,
    parse_census_geo,
    parse_census_part1,
    parse_census_part2,
    parse_census_part3,
)

BASE_URL = "https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/"


def get_state(state: str, max_attempts: int = 3) -> pa.Table:
    state_obj = us.states.lookup(state)
    if not state_obj:
        raise ValueError(f"Do not recognize state {state}")

    url = urljoin(BASE_URL, f'{state_obj.name.replace(" ", "_")}/')
    filename = f"{state_obj.abbr.lower()}2020.pl.zip"
    url = urljoin(url, filename)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        num_attempts = 0
        while num_attempts < max_attempts:
            with requests.get(url, stream=True) as response:
                if not response.ok:
                    num_attempts += 1
                    time.sleep(3)
                    continue

                with open(tmpdir / filename, "wb") as outfile:
                    for chunk in response.iter_content(chunk_size=8192):
                        outfile.write(chunk)
                break

        if num_attempts == max_attempts:
            raise EnvironmentError(f"Ran out of attempts to download {state} data")

        # Now parse the data
        with zipfile.ZipFile(tmpdir / filename, "r") as zipped:
            zipped.extractall(tmpdir)

        # Parse the files
        table1 = parse_census_part1(list(tmpdir.rglob("*12020.pl"))[0])
        table2 = parse_census_part2(list(tmpdir.rglob("*22020.pl"))[0])
        table3 = parse_census_part3(list(tmpdir.rglob("*32020.pl"))[0])
        tablegeo = parse_census_geo(list(tmpdir.rglob("*geo2020.pl"))[0])

        return combine_tables(table1, table2, table3, tablegeo)
