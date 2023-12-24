"""Functions to read parquet files"""

from typing import Callable, Union

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs

DatasetOptional = Union[ds.Dataset, None]
DataSetFactoryFunction = Callable[[str, pa.schema, fs.FileSystem], DatasetOptional]


def create_dataset_from_filesystem(
    path: str,
    schema: pa.schema,
    filesystem: fs.FileSystem,
    dataset_factory: DataSetFactoryFunction = ds.dataset,
) -> DatasetOptional:
    """
    Creates PyArrow dataset from local folder.
    """
    try:
        dataset = dataset_factory(source=path, schema=schema, filesystem=filesystem)
    except Exception as e:
        dataset = None

    return dataset
