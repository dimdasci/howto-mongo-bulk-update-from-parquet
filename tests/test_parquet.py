from unittest.mock import Mock

import pyarrow as pa
from pyarrow import fs

from src.func.parquet import create_dataset_from_filesystem


def test_create_dataset_from_filesystem():
    # Mocking the necessary objects
    path = "path/to/data"
    dummy_dataset = "dataset"
    schema = pa.schema([("column1", pa.int64()), ("column2", pa.string())])
    filesystem = Mock()
    dataset_factory_mock = Mock()
    dataset_factory_mock.return_value = dummy_dataset

    # Call the function with the mocked objects
    dataset = create_dataset_from_filesystem(
        path=path,
        schema=schema,
        filesystem=filesystem,
        dataset_factory=dataset_factory_mock,
    )

    # Verify that the dataset factory function was called with the correct arguments
    dataset_factory_mock.assert_called_once_with(
        source=path, schema=schema, filesystem=filesystem
    )

    # Verify that the dataset returned by the function is the same as
    # the one returned by the dataset factory function
    assert dataset == dummy_dataset
