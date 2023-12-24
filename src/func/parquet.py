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

    Args:
        path: Path to the folder containing the parquet files.
        schema: Schema of the parquet files.
        filesystem: PyArrow filesystem object.
        dataset_factory: Function to create the PyArrow dataset.

    Returns:
        PyArrow dataset object or None if the dataset creation fails
    """
    try:
        dataset = dataset_factory(source=path, schema=schema, filesystem=filesystem)
    except Exception as e:
        dataset = None

    return dataset
