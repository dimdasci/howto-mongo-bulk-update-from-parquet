import asyncio
import logging

import pytest
from pymongo.errors import BulkWriteError, OperationFailure
from pymongo.operations import UpdateMany, UpdateOne

from src.func.mongo import (
    create_update_task,
    get_bulk_write_statements,
    make_update_statement,
    run_update,
    update_record_batches,
)


def test_make_update_statement():
    # Test case with valid item, id_column, and fields
    item = {"id": 1, "name": "John", "age": 30}
    id_column = "id"
    fields = ["name", "age"]

    expected_result = UpdateOne(
        filter={"id": 1},
        update={
            "$set": {"name": "John", "age": 30},
            "$currentDate": {"updatedAt": True},
        },
        upsert=True,
    )

    result = make_update_statement(
        item=item, id_column=id_column, fields=fields, update_fn=UpdateOne
    )
    assert result == expected_result

    # Test case with missing id in the item
    item_missing_id = {"name": "Jane", "age": 25}
    result_missing_id = make_update_statement(item_missing_id, id_column, fields)
    assert result_missing_id is None

    # Test case with None fields
    fields_none = None
    result_fields_none = make_update_statement(
        item=item, id_column=id_column, fields=fields_none, update_fn=UpdateOne
    )
    assert result_fields_none is None

    # Test case with empty fields
    fields_empty = []
    result_fields_empty = make_update_statement(
        item=item, id_column=id_column, fields=fields_empty, update_fn=UpdateOne
    )
    assert result_fields_empty is None

    # Test case with custom update function
    def custom_update_fn(filter, update, upsert):
        return f"Custom Update: {filter}, {update}, {upsert}"

    result_custom_update_fn = make_update_statement(
        item, id_column, fields, custom_update_fn
    )
    expected_custom_result = "Custom Update: {'id': 1}, {'$set': {'name': 'John', 'age': 30}, '$currentDate': {'updatedAt': True}}, True"
    assert result_custom_update_fn == expected_custom_result


def test_get_bulk_write_statements():
    # Test case with valid items and make_update_fn
    items = [
        {"_id": 1, "name": "John", "age": 30},
        {"_id": 2, "name": "Jane", "age": 25},
        {"_id": 3, "name": "Doe", "age": 35},
    ]

    def make_update_fn(item):
        return UpdateOne(
            filter={"_id": item["_id"]},
            update={
                "$set": {"name": item["name"], "age": item["age"]},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        )

    expected_result = [
        UpdateOne(
            filter={"_id": 1},
            update={
                "$set": {"name": "John", "age": 30},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        ),
        UpdateOne(
            filter={"_id": 2},
            update={
                "$set": {"name": "Jane", "age": 25},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        ),
        UpdateOne(
            filter={"_id": 3},
            update={
                "$set": {"name": "Doe", "age": 35},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        ),
    ]

    result = list(get_bulk_write_statements(items, make_update_fn))
    assert result == expected_result

    # Test case with empty items
    empty_items = []
    result_empty_items = get_bulk_write_statements(empty_items, make_update_fn)
    assert result_empty_items is None

    # Test case with None items
    result_none_items = get_bulk_write_statements(None, make_update_fn)
    assert result_none_items is None

    # Test case with make_update_fn returning None for one item
    def make_update_fn_with_none(item):
        if item["_id"] == 2:
            return None
        return make_update_fn(item)

    expected_result_with_none = [
        UpdateOne(
            filter={"_id": 1},
            update={
                "$set": {"name": "John", "age": 30},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        ),
        # No UpdateOne for item with _id 2 due to make_update_fn returning None
        UpdateOne(
            filter={"_id": 3},
            update={
                "$set": {"name": "Doe", "age": 35},
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
        ),
    ]

    result_with_none = list(get_bulk_write_statements(items, make_update_fn_with_none))
    assert result_with_none == expected_result_with_none


class UpdateResultMock:
    def __init__(self, matched_count, modified_count, upserted_id):
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class AsyncIOMotorCollectionMock:
    async def bulk_write(self, update_statements, ordered):
        return UpdateResultMock(matched_count=2, modified_count=2, upserted_id=None)


@pytest.mark.asyncio
async def test_run_update():
    # Mocking the necessary objects
    logger = logging.getLogger("test_logger")
    update_statements = [
        {"_id": 1, "update": {"$set": {"name": "John"}}},
        {"_id": 2, "update": {"$set": {"name": "Jane"}}},
    ]
    mongo_collection = AsyncIOMotorCollectionMock()
    ordered = False

    # Test case with a successful bulk write
    result = await run_update(logger, update_statements, mongo_collection, ordered)
    expected_result = {"n_matched": 2, "n_modified": 2}
    assert result == expected_result

    # Test case with an empty update_statements list
    empty_update_statements = []
    result_empty = await run_update(
        logger, empty_update_statements, mongo_collection, ordered
    )
    assert result_empty is None

    # Test case with None update_statements
    result_none = await run_update(logger, None, mongo_collection, ordered)
    assert result_none is None


@pytest.mark.asyncio
async def test_create_update_task():
    # Mocking the necessary objects
    logger = logging.getLogger("test_logger")
    items = [
        {"_id": 1, "name": "John", "age": 30},
        {"_id": 2, "name": "Jane", "age": 25},
    ]
    mongo_collection = AsyncIOMotorCollectionMock()
    id_column = "_id"
    fields = ["name", "age"]
    update_fn = UpdateOne
    ordered = False

    # Test case with a successful async task creation
    task = create_update_task(
        logger, mongo_collection, items, id_column, fields, update_fn, ordered
    )
    assert isinstance(task, asyncio.Task)

    # Test case with empty items
    empty_items = []
    task_empty_items = create_update_task(
        logger, mongo_collection, empty_items, id_column, fields, update_fn, ordered
    )
    assert isinstance(task_empty_items, asyncio.Task)

    # Test case with None items
    task_none_items = create_update_task(
        logger, mongo_collection, None, id_column, fields, update_fn, ordered
    )
    assert isinstance(task_none_items, asyncio.Task)


@pytest.mark.asyncio
async def test_update_record_batches():
    # Mocking the necessary objects
    logger = logging.getLogger("test_logger")
    record_batches = [
        [
            {"_id": 1, "name": "John", "age": 30},
            {"_id": 2, "name": "Jane", "age": 25},
        ],
        [
            {"_id": 3, "name": "Doe", "age": 35},
            {"_id": 4, "name": "Smith", "age": 40},
        ],
    ]
    mongo_collection = AsyncIOMotorCollectionMock()
    id_column = "_id"
    fields = ["name", "age"]
    update_fn = UpdateOne
    ordered = False

    # Test case with successful update of record batches
    result = await update_record_batches(
        logger, mongo_collection, record_batches, id_column, fields, update_fn, ordered
    )
    expected_result = [
        {"n_matched": 2, "n_modified": 2},
        {"n_matched": 2, "n_modified": 2},
    ]
    assert result == expected_result

    # Test case with empty record batches
    empty_record_batches = []
    result_empty_batches = await update_record_batches(
        logger,
        mongo_collection,
        empty_record_batches,
        id_column,
        fields,
        update_fn,
        ordered,
    )
    assert result_empty_batches == []
