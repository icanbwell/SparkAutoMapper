from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import StructType, StructField, StringType
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)

from spark_auto_mapper.data_types.list import AutoMapperList

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
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

from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_concat_multiple_items_structs_different_elements_with_schema(
    spark_session: SparkSession,
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, None, "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df: DataFrame = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    schema: StructType = StructType(
        [
            StructField("id", StringType(), True),
            StructField("c", StringType(), True),
            StructField("b", StringType(), True),
        ]
    )

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        dst2=AutoMapperList(
            [
                AutoMapperDataTypeComplexBase(
                    id_=A.column("first_name"), b=A.column("last_name")
                ),
            ],
            children_schema=schema,
        ).concat(
            AutoMapperList(
                [
                    AutoMapperDataTypeComplexBase(
                        id_=A.column("first_name"), c=A.column("last_name")
                    ),
                ],
                children_schema=schema,
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    struct1 = struct(
        col("b.first_name").alias("id"),
        lit(None).alias("c"),
        col("b.last_name").alias("b"),
    )
    struct2 = struct(
        col("b.first_name").alias("id"),
        col("b.last_name").alias("c"),
        lit(None).alias("b"),
    )
    array1 = when(
        array(struct1).isNotNull(),
        filter(coalesce(array(struct1), array()), lambda x: x.isNotNull()),
    )
    array2 = when(
        array(struct2).isNotNull(),
        filter(coalesce(array(struct2), array()), lambda x: x.isNotNull()),
    )
    assert_compare_expressions(
        sql_expressions["dst2"], concat(array1, array2).alias("dst2")
    )
    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][0]
        == "Imran"
    )
    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][2]
        == "Qureshi"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][0]
        == "Michael"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][1] is None
    )
