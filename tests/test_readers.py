import tempfile
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from census2020 import parsers, readers
from census2020.constants import SummaryLevel


@pytest.fixture(scope="module")
def fixtures_path() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def dataset_location(fixtures_path: Path) -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)

        wyoming = parsers.parse_all(fixtures_path / "wy2020.pl.zip")
        rhode_island = parsers.parse_all(fixtures_path / "ri2020.pl.zip")
        pq.write_table(rhode_island, tmpdir / "ri.parquet")
        pq.write_table(wyoming, tmpdir / "wy.parquet")
        yield tmpdir


def test_read_filtered_dataset(dataset_location: Path):

    assert (
        len(
            readers.read_filtered_dataset(
                dataset_location, states=["ri", "wy"], level=SummaryLevel.STATE_COUNTY
            ).to_pandas()
        )
        == 28
    )
