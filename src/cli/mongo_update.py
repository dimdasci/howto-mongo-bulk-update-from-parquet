import logging
from asyncio import run

import click
from motor.motor_asyncio import AsyncIOMotorCollection
from pyarrow import fs
from watchtower import CloudWatchLogHandler

from src.config import config
from src.func.aws import set_env_to_credentials
from src.func.job import run_update
from src.func.log import LOGGER, close_handler, setup_logger
from src.func.mongo import get_mongo_collection


@click.command()
@click.option(
    "--path",
    "-p",
    type=click.Path(),
    default="assets/data/",
    help="Path to the directory with parquet files or a file",
    show_default=True,
)
# option for read batch size, int, default 10000
@click.option(
    "--batch-size",
    "-b",
    type=click.IntRange(1000, 100_000_000),
    default=10_000,
    help="Number of rows per batch",
    show_default=True,
)
# option for concurrent tasks, int, default 10
@click.option(
    "--concurrent-tasks",
    "-c",
    type=click.IntRange(1, 100),
    default=10,
    help="Number of concurrent update tasks",
    show_default=True,
)
@click.option("--profile", type=click.STRING, help="AWS profile, optional")
def main(
    path: str, batch_size: int, concurrent_tasks: int, profile: str = None
) -> None:
    """Updates mongo collection from parquet dataset"""

    # Set environment variables to AWS credentials from the session
    set_env_to_credentials(profile)

    # Create a logger object
    handler: CloudWatchLogHandler = setup_logger(logging.DEBUG)
    LOGGER.info(
        dict(
            stage="Start Job",
            params=dict(
                path=path, batch_size=batch_size, concurrent_tasks=concurrent_tasks
            ),
        )
    )

    collection: AsyncIOMotorCollection = get_mongo_collection(
        logger=LOGGER,
        connection=config.MONGO_CONNECTION_STRING,
        database=config.database,
        collection=config.collection,
    )

    # PyArrow filesystem object
    filesystem = fs.LocalFileSystem()

    run(
        run_update(
            logger=LOGGER,
            path=path,
            batch_size=batch_size,
            concurrent_tasks=concurrent_tasks,
            filesystem=filesystem,
            collection=collection,
        )
    )

    LOGGER.info(dict(stage="Finish Job"))
    close_handler(handler)


if __name__ == "__main__":
    main()
