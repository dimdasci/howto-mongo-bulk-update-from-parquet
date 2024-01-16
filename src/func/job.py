"""Module to run an update
"""
from logging import Logger

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne

from src.func.mongo import update_record_batches
from src.func.parquet import (
    create_dataset_from_filesystem,
    get_record_batch_iterator,
    get_sliced_iterator,
    transform_dict_to_list,
)


async def run_update(
    logger: Logger,
    path: str,
    batch_size: int,
    concurrent_tasks: int,
    filesystem: fs.LocalFileSystem,
    collection: AsyncIOMotorCollection,
) -> None:
    """
    Runs the bulk update process.
    """

    schema = pa.schema(
        [
            pa.field("_id", pa.string(), False),
            pa.field("feature_1", pa.float64(), True),
            pa.field("feature_2", pa.float64(), True),
            pa.field("feature_3", pa.float64(), True),
            pa.field("feature_4", pa.float64(), True),
            pa.field("score", pa.float64(), False),
        ]
    )

    # Create a PyArrow dataset object
    dataset = create_dataset_from_filesystem(
        logger=logger,
        path=path,
        schema=schema,
        filesystem=filesystem,
        dataset_factory=ds.dataset,
    )

    # Get an iterator over the dataset batches
    iterator = get_record_batch_iterator(
        dataset=dataset, schema=schema, batch_size=batch_size
    )

    # Slice iterator by mongo_batch_size and iterate over it
    mongo_iterator = get_sliced_iterator(
        record_batch_iterator=iterator, slice_size=concurrent_tasks
    )

    print(
        f"Read data in {batch_size} row batches and process in {concurrent_tasks} concurrent tasks"
    )
    logger.info(
        dict(
            stage="Start read and update",
            batch_size=batch_size,
            concurrent_tasks=concurrent_tasks,
        )
    )
    results = [
        await update_record_batches(
            logger=logger,
            index=si,
            mongo_collection=collection,
            record_batches=[
                transform_dict_to_list(record_batch.to_pydict())
                for record_batch in slice
            ],
            id_column=schema.names[0],
            fields=schema.names[1:],
            update_fn=UpdateOne,
            ordered=False,
        )
        for si, slice in enumerate(mongo_iterator)
    ]

    logger.info(
        dict(
            stage="Finish read and update",
            results=results,
        )
    )
