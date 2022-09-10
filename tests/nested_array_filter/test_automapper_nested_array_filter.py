from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, exists, filter, struct, transform, lit
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StructField,
    ArrayType,
    StringType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from tests.nested_array_filter.location import AutoMapperElasticSearchLocation
from tests.nested_array_filter.schedule import AutoMapperElasticSearchSchedule


def test_automapper_nested_array_filter_with_parent_column(
    spark_session: SparkSession,
) -> None:
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
                [{"name": "location-100"}, {"name": "location-200"}],
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

    mapper = AutoMapper(
        view="schedule", source_view="patients", keys=["row_id"]
    ).columns(
        location=A.column("location").select(
            AutoMapperElasticSearchLocation(
                name=A.field("name"),
                scheduling=A.nested_array_filter(
                    array_field=A.column("schedule"),
                    inner_array_field=A.field("actor"),
                    match_property="reference",
                    match_value=A.field("{parent}.name"),
                ).select_one(AutoMapperElasticSearchSchedule(name=A.field("name"))),
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    print("------COLUMN SPECS------")
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")
    assert_compare_expressions(
        sql_expressions["location"],
        transform(
            col("b.location"),
            lambda l: (
                struct(
                    l["name"].alias("name"),
                    transform(
                        filter(
                            col("b.schedule"),
                            lambda s: exists(
                                s["actor"],
                                lambda a: a["reference"] == l["name"],  # type: ignore
                            ),
                        ),
                        lambda s: struct(s["name"].alias("name")),
                    )[0].alias("scheduling"),
                )
            ),
        ).alias("___location"),
    )
    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    # result_df.printSchema()
    # result_df.show(truncate=False)
    location_row = result_df.collect()[0].location
    for index, location in enumerate(location_row):
        location_name = location.name
        location_scheduling = location.scheduling
        assert location_name == f"location-{index + 1}00"
        assert len(location_scheduling) == 1
        assert location_scheduling.name == f"schedule-{index + 1}"


def test_automapper_nested_array_filter_with_text(spark_session: SparkSession) -> None:
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
                [{"name": "location-100"}, {"name": "location-200"}],
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
                scheduling=A.nested_array_filter(
                    array_field=A.column("schedule"),
                    inner_array_field=A.field("actor"),
                    match_property="reference",
                    match_value=A.text("practitioner-role-200"),
                ).select_one(AutoMapperElasticSearchSchedule(name=A.field("name"))),
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    print("------COLUMN SPECS------")
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")
    assert_compare_expressions(
        sql_expressions["location"],
        transform(
            col("b.location"),
            lambda l: (
                struct(
                    l["name"].alias("name"),
                    transform(
                        filter(
                            col("b.schedule"),
                            lambda s: exists(
                                s["actor"],
                                lambda a: a["reference"]
                                == lit("practitioner-role-200").cast("string"),
                            ),
                        ),
                        lambda s: struct(s["name"].alias("name")),
                    )[0].alias("scheduling"),
                )
            ),
        ).alias("___location"),
    )
    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    print("------RESULTS------")
    # result_df.printSchema()
    # result_df.show(truncate=False)
    location_row = result_df.collect()[0].location
    for index, location in enumerate(location_row):
        location_name = location.name
        location_scheduling = location.scheduling
        assert location_name == f"location-{index + 1}00"
        assert len(location_scheduling) == 1
        assert location_scheduling.name == "schedule-2"
