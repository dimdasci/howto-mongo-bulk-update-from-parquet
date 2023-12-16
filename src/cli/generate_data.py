"""
Generates dummy data to use as input for the tutorial.
Saves the data in the assets/data directory in Parquet format.
"""

import uuid

import click
import numpy as np
import pyarrow as pa
from pyarrow import parquet as pq

######################
# Data type definition
######################

# Define schema entry type as a tuple of string, pyarrow data type and boolean
# indicating whether the field is nullable or not
SchemaEntry = tuple[str, pa.DataType, bool]
# Define schema type as a list of SchemaEntry
Schema = list[SchemaEntry]

#######################
# Constants definition
#######################

DATA_SCHEMA: Schema = [
    ("id", pa.string(), False),
    ("feature_1", pa.float64(), True),
    ("feature_2", pa.float64(), True),
    ("feature_3", pa.float64(), True),
    ("feature_4", pa.float64(), True),
    ("score", pa.float64(), False),
]

#######################
# Functions definition
#######################


def schema_as_pyarrow(fields: Schema) -> pa.Schema:
    """Converts a list of SchemaEntry into a pyarrow.Schema"""

    return pa.schema(
        [pa.field(name, data_type, nullable) for name, data_type, nullable in fields]
    )


def field_names(fields: Schema) -> list[str]:
    """Returns the list of field names from a Schema"""

    return [name for name, _, _ in fields]


def generate_data_batch(batch_size: int, data_schema: Schema) -> pa.Table:
    """For a given schema generates a batch of dummy data of size batch_size"""

    # For id we generate UUIDs, for the rest of the fields we generate random floats
    # For month we generate a random integer between 1 and 12
    id_column = pa.array([str(uuid.uuid4()) for _ in range(batch_size)])
    feature_columns = [pa.array(np.random.rand(batch_size)) for _ in range(4)]
    # The score column is a linear combination of the feature columns
    score_column = pa.array(np.random.rand(batch_size))
    # We concatenate all columns into a single table
    return pa.Table.from_arrays(
        [id_column] + feature_columns + [score_column],
        schema=schema_as_pyarrow(data_schema),
    )


def save_data_batch(
    batch: pa.Table, batch_num: int, path: str, data_schema: Schema
) -> None:
    """Saves a pyarrow table as a Parquet file in append mode"""

    # We use the pyarrow.parquet library to save the data
    parquet_writer = pq.ParquetWriter(
        f"{path}p_{batch_num:04d}.parquet", schema_as_pyarrow(data_schema)
    )
    parquet_writer.write_table(batch)
    parquet_writer.close()


def generate_data(
    n_batches: int, batch_size: int, path: str, data_schema: Schema
) -> None:
    """Generates n_batches of data of size batch_size and saves them in path"""

    # tasks: list[asyncio.Task] = []

    for i in range(n_batches):
        batch: pa.Table = generate_data_batch(batch_size, data_schema=data_schema)
        save_data_batch(batch=batch, batch_num=i, data_schema=data_schema, path=path)

    # await asyncio.gather(*tasks)


#######################
# Main function
#######################


@click.command()
@click.option(
    "--n-batches",
    type=click.IntRange(1, 100),
    default=10,
    help="Number of batches to generate",
    show_default=True,
)
@click.option(
    "--batch-size",
    type=click.IntRange(1000, 100000000),
    default=1000,
    help="Number of rows per batch",
    show_default=True,
)
@click.option(
    "--path",
    type=click.Path(),
    default="assets/data/",
    help="Path to save the data",
    show_default=True,
)
def main(n_batches: int, batch_size: int, path: str) -> None:
    """Generates dummy data to use as input for the tutorial"""

    print(
        f"Generating {n_batches} batches of size {batch_size} and saving to {path}..."
    )

    generate_data(n_batches, batch_size, path, data_schema=DATA_SCHEMA)

    print("Done!")


if __name__ == "__main__":
    main()
