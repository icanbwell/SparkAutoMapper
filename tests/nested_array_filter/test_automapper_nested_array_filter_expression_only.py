from os import path
from pathlib import Path
from shutil import rmtree

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import transform, col, struct, exists, filter
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StructField,
    ArrayType,
    StringType,
)


def test_automapper_nested_array_filter_expression_only(
    spark_session: SparkSession,
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)

    schema = StructType(
        [
            StructField("row_id", dataType=IntegerType(), nullable=False),
            StructField(
                "location",
                dataType=ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                        ]
                    )
                ),
            ),
            StructField(
                "schedule",
                dataType=ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField(
                                "actor",
                                ArrayType(
                                    StructType(
                                        [StructField("reference", StringType(), True)]
                                    ),
                                    True,
                                ),
                            ),
                        ]
                    )
                ),
            ),
            StructField(
                "single_level",
                dataType=ArrayType(
                    StructType(
                        [
                            StructField("reference", StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    spark_session.createDataFrame(
        [
            (
                1,
                [{"name": "location-100"}],
                [
                    {
                        "name": "schedule-1",
                        "actor": [
                            {"reference": "location-100"},
                            {"reference": "practitioner-role-100"},
                        ],
                    },
                    {
                        "name": "schedule-2",
                        "actor": [
                            {"reference": "location-200"},
                            {"reference": "practitioner-role-200"},
                        ],
                    },
                ],
                [{"reference": "location-100"}, {"reference": "location-200"}],
            )
        ],
        schema,
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    source_df.printSchema()
    source_df.show(truncate=False)

    my_expression = transform(
        col("location"),
        lambda l: (
            struct(
                l["name"].alias("name"),
                transform(
                    filter(
                        col("schedule"),
                        lambda s: exists(
                            s["actor"],
                            lambda a: a["reference"] == l["name"],  # type: ignore
                        ),
                    ),
                    lambda s: struct(s["name"].alias("name")),
                )[0].alias("scheduling"),
            )
        ),
    )

    print(f"{my_expression}")

    source_df.select(my_expression).show()
