"""Functions to read parquet files"""

import logging
from itertools import islice
from typing import Any, Callable, Generator, Union

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs

DatasetOptional = Union[ds.Dataset, None]
DataSetFactoryFunction = Callable[[str, pa.schema, fs.FileSystem], DatasetOptional]
DataSetProcessFunction = Callable[[dict, Any], Any]
DictOptional = Union[dict[str, list[Any]], None]
ListOptional = Union[list[dict[str, Any]], None]

def create_dataset_from_filesystem(
    logger: logging.Logger,
    path: str,
    schema: pa.Schema,
    filesystem: fs.FileSystem,
    dataset_factory: DataSetFactoryFunction = ds.dataset,
) -> DatasetOptional:
    """
    Creates PyArrow dataset from local folder.

    Args:
        logger: Logger object.
        path: Path to the folder containing the parquet files.
        schema: Schema of the parquet files.
        filesystem: PyArrow filesystem object.
        dataset_factory: Function to create the PyArrow dataset.

    Returns:
        PyArrow dataset object or None if the dataset creation fails
    """
    try:
        logger.debug(f"Creating dataset from {path}, schema: {schema}")
        dataset = dataset_factory(source=path, schema=schema, filesystem=filesystem)
    except Exception as e:
        logger.error(f"Failed to create dataset from {path}: {e}")
        dataset = None

    return dataset


# returns iterator of RecordBatch
def get_record_batch_iterator(
    dataset: DatasetOptional, schema: pa.Schema, batch_size: int
) -> Generator[pa.RecordBatch, None, None]:
    """
    Returns iterator over dataset batches.

    Args:
        dataset: PyArrow dataset object.
        schema: Schema of the parquet files.
        batch_size: Size of the batches.

    Returns:
        Iterator over dataset batches.
    """

    # if dataset is None return empty iterator
    # else return iterator over dataset batches
    return (
        iter(
            dataset.to_batches(
                columns=schema.names, batch_size=batch_size, use_threads=True
            )
        )
        if dataset
        else iter([])
    )


def get_mongo_update_iterator(
    record_batch_iterator: Generator[pa.RecordBatch, None, None], mongo_batch_size: int
) -> Generator[pa.RecordBatch, None, None]:
    """
    Returns iterator over dataset batches.

    Args:
        record_batch_iterator: PyArrow RecordBatch Iterator.
        mongo_batch_size: Size of the batches.

    Returns:
        Iterator over dataset batches.
    """

    # if dataset is None return empty iterator
    # else return iterator over dataset batches
    return (
        iter(islice(record_batch_iterator, 0, None, mongo_batch_size))
        if record_batch_iterator
        else iter([])
    )

def transform_dict_to_list(dict_data: DictOptional) -> ListOptional:
    """
    Transform a dictionary with table column names as keys and rows as values
    to a list of dictionaries where each dictionary represents a row.

    Args:
        dict_data (dict): Dictionary with table column names as keys and rows as values.

    Returns:
        list: List of dictionaries, where each dictionary represents a row.
    """
    if not dict_data:
        return None

    # Get column names from the first row
    columns = list(dict_data.keys())

    # Determine the number of rows (assuming all rows have the same length)
    num_rows = len(next(iter(dict_data.values())))

    # Create a list of dictionaries for each row
    result = [
        {column: dict_data[column][row_index] for column in columns}
        for row_index in range(num_rows)
    ]

    return result