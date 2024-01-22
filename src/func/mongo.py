"""
Functions to interact with MongoDB
"""
from asyncio import Task, create_task, gather
from logging import Logger
from time import time
from typing import Any, Callable, Optional, Union

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

# Global variable to store the start time of the job
INIT_TIME = time()


def make_update_statement(
    logger: Logger,
    slice_index: int,
    task_index: int,
    item: ItemOptional,
    id_column: str,
    fields: list[str],
    update_fn: UpdateFunction = UpdateOne,
) -> UpdateOptional:
    """
    Creates a PyMongo UpdateOne statement for a given item.

    Args:
        logger: Logger object.
        slice_index: Index of the slice.
        task_index: Index of the task.
        item: Dictionary with the item to update.
        id_column: Name of the id column.
        fields: List of fields to update.
        update_fn: PyMongo update function.

    Returns:
        PyMongo UpdateOne statement.
    """
    print(item)
    assert isinstance(item, dict)

    if item is None:
        logger.error(dict(msg="No item to make update statement"))
        return None

    id = item.get(id_column)
    if id is None:
        logger.error(dict(msg="No id to make update statement", id_column=id_column))
        return None

    if fields is None or len(fields) == 0:
        logger.error(dict(msg="No fields to update"))
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
    logger.debug(
        dict(
            stage="Made update statement",
            filter=filter,
            update=update,
            slice_index=slice_index,
            task_index=task_index,
            status="Success",
        )
    )

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
    logger: Logger,
    update_statements: BulkUpdateOptional,
    mongo_collection: AsyncIOMotorCollection,
    ordered: bool = False,
) -> UpdateResultOptional:
    """
    Runs a bulk write operation on a MongoDB collection.
    """

    if update_statements is None:
        logger.error(
            dict(
                msg="No update statements",
                status="Failed",
            )
        )
        return None

    sequence = list(update_statements)
    if len(sequence) == 0:
        logger.error(
            dict(
                msg="Update statements list is empty",
                status="Failed",
            )
        )
        return None

    # prepare status dict for logging
    status = dict(
        stage="MongoDB bulk write",
        n_statements=len(sequence),
    )

    try:
        write_result = await mongo_collection.bulk_write(sequence, ordered=ordered)
        result = {
            "n_matched": write_result.matched_count,
            "n_modified": write_result.modified_count,
            "n_upserted": write_result.upserted_count,
            "n_inserted": write_result.inserted_count,
        }
        status.update(dict(status="Success", **result))

    except BulkWriteError as bwe:
        # Log the details of the error and return the result
        logger.error(dict(msg="MongoDB bulk write error", error=bwe.details))
        status.update(dict(status="Failure"))
        result = {
            "n_matched": bwe.details["nMatched"],
            "n_modified": bwe.details["nModified"],
            "n_upserted": bwe.details["nUpserted"],
            "n_inserted": bwe.details["nInserted"],
        }
        status.update(dict(status="Success", **result))
    except OperationFailure as of:
        logger.error(dict(msg="MongoDB bulk write error", error=of.details))
        status.update(dict(status="Failure"))
        result = None
    except Exception as e:
        logger.error(dict(msg="MongoDB bulk write error", error=e))
        status.update(dict(status="Failure"))
        result = None

    logger.debug(status)

    return result


def create_update_task(
    logger: Logger,
    slice_index: int,
    task_index: int,
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
    assert isinstance(items, list)

    update_statements: BulkUpdateOptional = get_bulk_write_statements(
        items=items,
        make_update_fn=lambda item: make_update_statement(
            logger=logger,
            slice_index=slice_index,
            task_index=task_index,
            item=item,
            id_column=id_column,
            fields=fields,
            update_fn=update_fn,
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
    logger.debug(
        dict(
            stage="Update task created",
            slice_index=slice_index,
            task_index=task_index,
            status="Success",
        )
    )

    return task


async def update_record_batches(
    logger: Logger,
    index: int,
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

    tasks = [
        create_update_task(
            logger=logger,
            slice_index=index,
            task_index=ti,
            mongo_collection=mongo_collection,
            items=record_batch,
            id_column=id_column,
            fields=fields,
            update_fn=update_fn,
            ordered=ordered,
        )
        for ti, record_batch in enumerate(record_batches)
    ]

    # store start time
    start_time = round(time() - INIT_TIME, 1)  # seconds

    logger.info(
        dict(
            stage="Created concurrent tasks",
            slice_index=index,
            start_time=start_time,
            n_tasks=len(tasks),
            status="Success",
        )
    )

    results = await gather(*tasks)

    logger.info(
        dict(
            stage="Executed concurrent tasks",
            index=index,
            finish_time=round(time() - INIT_TIME, 1),  # seconds
            duration_sec=round(time() - INIT_TIME - start_time, 1),  # seconds
            n_results=len(results),
            status="Success",
        )
    )

    return results


def get_mongo_collection(
    logger: Logger, connection: str, database: str, collection: str
) -> Optional[AsyncIOMotorCollection]:
    """
    Returns a MongoDB collection object.
    """

    try:
        client = AsyncIOMotorClient(connection)
        mongo_collection = client[database][collection]
    except Exception as e:
        logger.error(
            dict(
                msg="Failed to get mongo collection",
                database=database,
                collection=collection,
                error=e,
            )
        )
        mongo_collection = None

    logger.info(
        dict(
            stage="Get mongo collection",
            database=database,
            collection=collection,
            status="Success" if mongo_collection is not None else "Failed",
        )
    )
    return mongo_collection
