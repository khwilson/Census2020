# Helper functions for Census 2020 data

Every decade the US Census Bureau releases data from its [decennial census](https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html).
However, the files they provide are quite complicated. And while they provide [SAS](https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020PL_SAS_import_scripts.zip) and [R](https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020PL_R_import_scripts.zip), they don't provide any help for Python.

This package provides some convenience functions for playing around with all of this
Census data in Python.

## Requirements

We require Python 3.7.1 or above. This package does use [pyarrow](https://arrow.apache.org/docs/python/install.html) to make manipulating these large data sets easier. However, on some systems, you may encounter installation troubles. If you do, feel free to file an issue!

To install the package, simply run

```bash
pip install census2020
```

## Usage

### Getting the data

To use this package, you should first download the Census data. We've included a simple
CLI for you to grab all of the data and preprocess it:

```bash
census2020 pull-all --output data
```

Here `data` is a folder into which all the processed data will be dumped. WARNING: It
totals about 1.4GB after it's processed.

If for some reason CLI doesn't work, you can pull it by hand as follows:

```python
from pathlib import Path

import pyarrow.parquet as pq
import us

from census2020 import downloader

output_dir = Path("data")

for state in sorted(set(us.STATES) | {us.states.DC}):
    print(f"Downloading {state.name}...")
    table = downloader.get_state(state.abbr)
    pq.write_table(table, output_dir / f"{state.abbr.lower()}.parquet")
    print(f"Done with {state.name}")
```

### Reading the data

Reading in all the data into memory can be a bit of a difficult task, so we have
provided some interfaces to `pyarrow`'s filtering features to help.

For example, suppose you wanted the total population of people who identify as both
White and Asian in all Census Tracts in Kentucky, Indiana, and Ohio. Assuming you
have downloaded all the data, you can run the following code:

```python
from census2020 import readers
from census2020.constants import SummaryLevel

df = readers.read_filtered_dataset(
    "data",
    states=["KY", "IN", "OH"],
    levels=SummaryLevel.STATE_COUNTY_TRACT,
    columns="P0010013",
).to_pandas()
```

Here `"data"` is the location to which you downloaded the Census data.

Each of `states`, `columns`, and `levels` can be either singular values or lists of
values. If no value is specified, then all states, columns, and levels available
will be returned.

## Codebook

More detail on the information in these files is available from the [Census Bureau](https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html). In particular, a summary of the fields meanings is available in [this Excel file](https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020_PLSummaryFile_FieldNames.xlsx), reporduced in this repo as `field_names.xlsx`.

## License

MIT