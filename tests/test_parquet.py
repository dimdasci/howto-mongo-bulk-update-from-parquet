from unittest.mock import Mock

import pyarrow as pa
import pyarrow.dataset as ds

from src.func.parquet import (
    create_dataset_from_filesystem,
    get_mongo_update_iterator,
    get_record_batch_iterator,
    transform_dict_to_list,
)


def test_create_dataset_from_filesystem():
    # Mocking the necessary objects
    path = "path/to/data"
    schema = pa.schema([("column1", pa.int64()), ("column2", pa.string())])
    empty_table = pa.table([[], []], schema=schema)
    dummy_dataset = ds.dataset([empty_table])
    filesystem = Mock()
    logger = Mock()

    # Mocking the dataset factory function to return a dummy dataset
    dataset_factory_mock = Mock()
    dataset_factory_mock.return_value = dummy_dataset

    # Mocking another dataset factory that raises an exception
    dataset_factory_exception_mock = Mock(
        side_effect=Exception("Dataset creation failed")
    )

    # Call the function with the mocked objects
    dataset = create_dataset_from_filesystem(
        logger=logger,
        path=path,
        schema=schema,
        filesystem=filesystem,
        dataset_factory=dataset_factory_mock,
    )

    dataset_exception = create_dataset_from_filesystem(
        logger=logger,
        path=path,
        schema=schema,
        filesystem=filesystem,
        dataset_factory=dataset_factory_exception_mock,
    )

    # Verify that the dataset factory function was called with the correct arguments
    dataset_factory_mock.assert_called_once_with(
        source=path, schema=schema, filesystem=filesystem
    )

    # Verify that the dataset returned by the function is the same as
    # the one returned by the dataset factory function
    assert dataset == dummy_dataset

    # Verify that the dataset returned by the function is None
    assert dataset_exception is None


def test_dataset_batches():
    # Mocking the necessary objects
    batch_size = 2

    # Create a dummy PyArrow schema for testing
    schema = pa.schema([("column1", pa.int64()), ("column2", pa.string())])

    # Create a dummy PyArrow table for testing
    table = pa.table(
        {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]},
        schema=schema,
    )

    # Create a dummy PyArrow dataset with the table
    dataset = ds.dataset(table)

    # Call the function with the mocked objects
    batches = list(get_record_batch_iterator(dataset, schema, batch_size))

    # Verify that the correct batches are returned
    assert len(batches) == 3  # Dataset size is 5, batch size is 2 -> 3 batches
    assert isinstance(batches[0], pa.RecordBatch)
    assert batches[0].num_rows == 2  # Batch size is 2

    # Test with None dataset
    empty_batches = list(get_record_batch_iterator(None, schema, batch_size))
    assert len(empty_batches) == 0  # No batches should be returned for None dataset


def test_get_mongo_update_iterator():
    # Mocking the necessary objects
    batch_size = 2

    # Create a dummy PyArrow schema for testing
    schema = pa.schema([("column1", pa.int64()), ("column2", pa.string())])

    # Create a dummy PyArrow table for testing
    table = pa.table(
        {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]},
        schema=schema,
    )

    # Create a dummy PyArrow dataset with the table
    dataset = ds.dataset(table)

    # Call the function with the mocked objects
    record_iterator = get_record_batch_iterator(dataset, schema, batch_size)

    # Call the function with the mocked objects
    mongo_update_iterator = get_mongo_update_iterator(record_iterator, batch_size)

    # Verify that the correct batches are returned
    batches = list(mongo_update_iterator)
    assert len(batches) == 2  # Record batch size is 3, batch size is 2 -> 2 batches
    assert isinstance(batches[0], pa.RecordBatch)
    assert batches[0].num_rows == 2  # Assuming the dataset size is 2

    # Test with None record_batch_iterator
    empty_iterator = get_mongo_update_iterator(None, batch_size)
    assert (
        list(empty_iterator) == []
    )  # No batches should be returned for None record_batch_iterator

def test_transform_dict_to_list():
    # Test with a dictionary containing columns 'ID', 'Name', and 'Age'
    input_dict = {
        'ID': [1, 2, 3],
        'Fruit': ['Apple', 'Orange', 'Plum'],
        'Color': ['Green', 'Orange', 'Purple']
    }

    expected_result = [
        {'ID': 1, 'Fruit': 'Apple', 'Color': 'Green'},
        {'ID': 2, 'Fruit': 'Orange', 'Color': 'Orange'},
        {'ID': 3, 'Fruit': 'Plum', 'Color': 'Purple'}
    ]

    result_list = transform_dict_to_list(input_dict)

    assert result_list == expected_result

    # Test with an empty dictionary
    empty_dict = {}
    assert transform_dict_to_list(empty_dict) == None

    # Test with a dictionary with one column
    single_column_dict = {'ID': [1, 2, 3]}
    expected_single_column_result = [{'ID': 1}, {'ID': 2}, {'ID': 3}]
    assert transform_dict_to_list(single_column_dict) == expected_single_column_result
