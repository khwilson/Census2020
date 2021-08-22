from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import us

from census2020.types import FilenameType


def read_filtered_dataset(
    filename: FilenameType,
    states: Optional[List[str]] = None,
    level: Optional[str] = None,
) -> pa.Table:
    basedir = Path(filename)

    states = states or []
    state_objs = [us.states.lookup(state) for state in states]
    if state_objs:
        paths = [basedir / f"{state.abbr.lower()}.parquet" for state in state_objs]
    else:
        paths = basedir
    print(paths)
    dataset = ds.dataset(paths, format="parquet")

    filter_expression = ds.scalar(1) == ds.scalar(1)  # Just get a TRUE to & to
    if level is not None:
        filter_expression &= ds.field("SUMLEV") == level

    return dataset.to_table(filter=filter_expression)
