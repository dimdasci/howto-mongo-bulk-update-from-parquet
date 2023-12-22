"""Functions to read parquet files"""

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs


def create_dataset_from_filesystem(
    path: str,
    schema: pa.schema,
    filesystem: fs.FileSystem,
    dataset_factory: callable = ds.dataset,
):
    """
    Creates PyArrow dataset from local folder.
    """
    dataset = dataset_factory(source=path, schema=schema, filesystem=filesystem)
    return dataset
