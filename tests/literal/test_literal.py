from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame, Row

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.expression_comparer import assert_expressions_are_equal


def test_auto_mapper_datatype_literal(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        dst1="src1",
        dst2=AutoMapperDataTypeLiteral(None),
        dst3=AutoMapperDataTypeLiteral(""),
        dst4=AutoMapperDataTypeLiteral("literal"),
        dst5=AutoMapperDataTypeLiteral(1234),
        dst6=AutoMapperDataTypeLiteral(0),
    )

    assert isinstance(mapper, AutoMapper)

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)

    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_expressions_are_equal(sql_expressions["dst1"], lit("src1").alias("dst1"))
    assert_expressions_are_equal(sql_expressions["dst2"], lit(None).alias("dst2"))
    assert_expressions_are_equal(sql_expressions["dst3"], lit("").alias("dst3"))
    assert_expressions_are_equal(sql_expressions["dst4"], lit("literal").alias("dst4"))
    assert_expressions_are_equal(sql_expressions["dst5"], lit(1234).alias("dst5"))
    assert_expressions_are_equal(sql_expressions["dst6"], lit(0).alias("dst6"))

    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    result_df.printSchema()
    result_df.show()

    result = result_df.collect()

    assert result == [
        Row(
            member_id=1,
            dst1="src1",
            dst2=None,
            dst3="",
            dst4="literal",
            dst5=1234,
            dst6=0,
        ),
        Row(
            member_id=2,
            dst1="src1",
            dst2=None,
            dst3="",
            dst4="literal",
            dst5=1234,
            dst6=0,
        ),
    ]
