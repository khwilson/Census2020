import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from census2020 import parsers


@pytest.fixture(scope="module")
def fixtures_path() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def unzipped_fixture(fixtures_path: Path) -> Path:
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(fixtures_path / "wy2020.pl.zip", "r") as zipped_file:
            zipped_file.extractall(tmpdir)
        yield Path(tmpdir)


def test_parse_census_geo(unzipped_fixture: Path):
    filename = list(unzipped_fixture.rglob("*geo2020.pl"))[0]
    df = parsers.parse_census_geo(filename).to_pandas()
    assert (df["STUSAB"] == "WY").all()
    assert "COUNTYCC" in df.columns


def test_parse_census_part1(unzipped_fixture: Path):
    filename = list(unzipped_fixture.rglob("*12020.pl"))[0]
    df = parsers.parse_census_part1(filename).to_pandas()
    assert (df["STUSAB"] == "WY").all()
    assert "P0010001" in df.columns


def test_parse_census_part2(unzipped_fixture: Path):
    filename = list(unzipped_fixture.rglob("*22020.pl"))[0]
    df = parsers.parse_census_part2(filename).to_pandas()
    assert (df["STUSAB"] == "WY").all()
    assert "P0030001" in df.columns


def test_parse_census_part3(unzipped_fixture: Path):
    filename = list(unzipped_fixture.rglob("*32020.pl"))[0]
    df = parsers.parse_census_part3(filename).to_pandas()
    assert (df["STUSAB"] == "WY").all()
    assert "P0050001" in df.columns
