from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, hash, abs as abs_, concat
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_hash_abs(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "54"),
            (2, "Vidal", "67"),
            (3, "Vidal", None),
            (4, None, None),
        ],
        ["member_id", "last_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    source_df = source_df.withColumn("my_age", col("my_age").cast("int"))

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # create a function that returns the columns to hash
    def get_columns_to_hash() -> AutoMapperTextLikeBase:
        return A.concat(A.column("my_age"), A.column("last_name"))

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(age=A.hash_abs(get_columns_to_hash()))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        abs_(hash(concat(col("b.my_age"), col("b.last_name"))))
        .cast("string")
        .alias("age"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age").collect()[0][0] == 181944084
    assert result_df.where("member_id == 2").select("age").collect()[0][0] == 1424244624
    assert result_df.where("member_id == 3").select("age").collect()[0][0] == 42

    assert dict(result_df.dtypes)["age"] == "int"
