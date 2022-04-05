from os import path
from pathlib import Path
from shutil import rmtree
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StructField,
    ArrayType,
    StringType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from tests.array_filter.location import AutoMapperElasticSearchLocation
from tests.array_filter.schedule import AutoMapperElasticSearchSchedule


def test_automapper_array_filter(spark_session: SparkSession) -> None:
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
                [{"name": "location-100"},
                 {"name": "location-200"}],
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

    mapper = AutoMapper(
        view="schedule", source_view="patients", keys=["row_id"]
    ).columns(
        location=A.column("location").select(
            AutoMapperElasticSearchLocation(
                name=A.field("name"),
                scheduling=A.array_filter(
                    array_field= A.column("schedule"),
                    inner_array_field=A.field("actor"),
                    match_property="reference",
                    match_value=A.field("{parent}.name"),
                ).select_one(AutoMapperElasticSearchSchedule(name=A.field("name"))),
            )
        )
    )
    # mapper = AutoMapper(
    #     view="schedule", source_view="patients", keys=["row_id"]
    # ).columns(
    #     location=A.array_filter(
    #                 array_field=schedule,
    #                 inner_array_field=A.field("actor"),
    #                 match_property="reference",
    #                 match_value=A.column("location").select_one(A.field("name")),
    #             )
    # )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    print("------COLUMN SPECS------")
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    print("------RESULTS------")
    result_df.coalesce(1).write.json(str(temp_folder))
    result_df.printSchema()
    result_df.show(truncate=False)
