from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import (
    lit,
    concat,
    array,
    col,
    struct,
    when,
    filter,
    coalesce,
)

from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from tests.conftest import clean_spark_session


def test_auto_mapper_concat_multiple_items_structs_different_elements(
    spark_session: SparkSession,
) -> None:
    # Arrange
    clean_spark_session(spark_session)
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, None, "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", enable_schema_pruning=True
    ).columns(
        dst2=AutoMapperList(
            [
                AutoMapperDataTypeComplexBase(
                    a=A.column("first_name"), b=A.column("last_name")
                )
            ],
        ).concat(
            AutoMapperList(
                [
                    AutoMapperDataTypeComplexBase(
                        a=A.column("first_name"), c=A.column("last_name")
                    ),
                ],
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    array1 = when(
        array(
            struct(
                col("b.first_name").alias("a"),
                col("b.last_name").alias("b"),
                lit(None).alias("c"),
            ),
        ).isNotNull(),
        filter(
            coalesce(
                array(
                    struct(
                        col("b.first_name").alias("a"),
                        col("b.last_name").alias("b"),
                        lit(None).alias("c"),
                    ),
                ),
                array(),
            ),
            lambda x: x.isNotNull(),
        ),
    )
    array2 = when(
        array(
            struct(
                col("b.first_name").alias("a"),
                lit(None).alias("b"),
                col("b.last_name").alias("c"),
            ),
        ).isNotNull(),
        filter(
            coalesce(
                array(
                    struct(
                        col("b.first_name").alias("a"),
                        lit(None).alias("b"),
                        col("b.last_name").alias("c"),
                    ),
                ),
                array(),
            ),
            lambda x: x.isNotNull(),
        ),
    )
    assert_compare_expressions(
        sql_expressions["dst2"], concat(array1, array2).alias("dst2")
    )

    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][0]
        == "Imran"
    )
    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][1]
        == "Qureshi"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][0]
        == "Michael"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][1] is None
    )
