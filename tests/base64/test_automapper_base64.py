from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import base64

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_base64(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "This is data 1"),
            (2, "This is data 2"),
        ],
        ["id", "data"],
    ).createOrReplaceTempView("responses")

    source_df: DataFrame = spark_session.table("responses")

    df = source_df.select("id")
    df.createOrReplaceTempView("content")

    # Act
    mapper = AutoMapper(view="content", source_view="responses", keys=["id"]).columns(
        encoded_column=A.base64(A.column("data"))
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["encoded_column"], base64(col("b.data")).alias("encoded_column")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()
    assert (
        result_df.where("id == 1").select("encoded_column").collect()[0][0]
        == "VGhpcyBpcyBkYXRhIDE="
    )
    assert (
        result_df.where("id == 2").select("encoded_column").collect()[0][0]
        == "VGhpcyBpcyBkYXRhIDI="
    )
