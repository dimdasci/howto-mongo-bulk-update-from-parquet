"""
Functions to interact with MongoDB
"""
import logging
from asyncio import Task, create_task, gather
from typing import Any, Callable, Union

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import UpdateMany, UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure

from src.func.parquet import ListOptional

UpdateFunction = Union[UpdateOne, UpdateMany]
UpdateOptional = Union[UpdateFunction, None]
BulkUpdateOptional = Union[list[UpdateFunction], None]
ItemOptional = Union[dict[str, Any], None]
MakeUpdateFunction = Callable[
    [ItemOptional, str, list[str], UpdateFunction], UpdateOptional
]
UpdateResultOptional = Union[dict[str, int], None]


def make_update_statement(
    item: ItemOptional,
    id_column: str,
    fields: list[str],
    update_fn: UpdateFunction = UpdateOne,
) -> UpdateOptional:
    """
    Creates a PyMongo UpdateOne statement for a given item.

    Args:
        item: Dictionary with the item to update.
        id_column: Name of the id column.
        fields: List of fields to update.
    """

    if item is None:
        return None

    id = item.get(id_column)
    if id is None:
        return None

    if fields is None or len(fields) == 0:
        return None

    filter = {id_column: id}
    update = {
        "$set": {
            field: item.get(field) for field in fields if item.get(field) is not None
        },
        "$currentDate": {
            "updatedAt": True,
        },
    }
    return update_fn(filter=filter, update=update, upsert=True)


def get_bulk_write_statements(
    items: ListOptional, make_update_fn: MakeUpdateFunction
) -> BulkUpdateOptional:
    """
    Creates a list of PyMongo UpdateOne statements for a given list of items.

    Args:
        items: List of dictionaries with the items to update.
        make_update_fn: Function to create the PyMongo UpdateOne statement.

    Returns:
        List of PyMongo UpdateOne or UpdateMany statements.
    """

    if items is None or len(items) == 0:
        return None

    optional_updates: list[UpdateOptional] = map(make_update_fn, items)
    return filter(lambda update: update is not None, optional_updates)


async def run_update(
    logger: logging.Logger,
    update_statements: BulkUpdateOptional,
    mongo_collection: AsyncIOMotorCollection,
    ordered: bool = False,
) -> UpdateResultOptional:
    """
    Runs a bulk write operation on a MongoDB collection.
    """

    if update_statements is None:
        return None

    sequence = list(update_statements)
    if len(sequence) == 0:
        return None

    try:
        logger.debug(f"MongoDB bulk write sequence of {len(sequence)} statements.")
        write_result = await mongo_collection.bulk_write(sequence, ordered=ordered)
        result = {
            "n_matched": write_result.matched_count,
            "n_modified": write_result.modified_count,
            "n_upserted": write_result.upserted_count,
            "n_inserted": write_result.inserted_count,
        }
        logger.debug(
            f"MongoDB bulk write done: {result['n_matched']} matched, "
            f"{result['n_modified']} modified, "
            f"{result['n_inserted']} inserted, "
            f"{result['n_upserted']} upserted."
        )
    except BulkWriteError as bwe:
        logger.error(f"MongoDB bulk write error: {bwe.details}")
        result = None
    except OperationFailure as of:
        logger.error(f"MongoDB operation failure: {of.details}")
        result = None
    except Exception as e:
        logger.error(f"MongoDB unknown error: {e}")
        result = None

    return result


def create_update_task(
    logger: logging.Logger,
    mongo_collection: AsyncIOMotorCollection,
    items: ListOptional,
    id_column: str,
    fields: list[str],
    update_fn: UpdateFunction = UpdateOne,
    ordered: bool = False,
) -> Task:
    """
    Creates an async task to run a bulk write operation on a MongoDB collection.
    """

    update_statements: BulkUpdateOptional = get_bulk_write_statements(
        items=items,
        make_update_fn=lambda item: make_update_statement(
            item=item, id_column=id_column, fields=fields, update_fn=update_fn
        ),
    )

    task: Task = create_task(
        run_update(
            logger=logger,
            update_statements=update_statements,
            mongo_collection=mongo_collection,
            ordered=ordered,
        )
    )

    return task


async def update_record_batches(
    logger: logging.Logger,
    mongo_collection: AsyncIOMotorCollection,
    record_batches: list[ListOptional],
    id_column: str,
    fields: list[str],
    update_fn: UpdateFunction = UpdateOne,
    ordered: bool = False,
) -> UpdateResultOptional:
    """
    Updates a list of record batches in a MongoDB collection.
    """

    logger.info("Starting bulk update process...")
    tasks = [
        create_update_task(
            logger=logger,
            mongo_collection=mongo_collection,
            items=record_batch,
            id_column=id_column,
            fields=fields,
            update_fn=update_fn,
            ordered=ordered,
        )
        for record_batch in record_batches
    ]
    logger.info(f"Created {len(tasks)} concurrent tasks.")
    results = await gather(*tasks)
    logger.info("Finished bulk update process.")

    return results


def get_mongo_collection(
    connection: str, database: str, collection: str
) -> AsyncIOMotorCollection:
    """
    Returns a MongoDB collection object.
    """

    client = AsyncIOMotorClient(connection)
    mongo_collection = client[database][collection]

    return mongo_collection
