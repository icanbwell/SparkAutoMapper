from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, lit
from pytest import approx

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_amount(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54.45"),
            (2, "Vidal", "Michael", "67.67"),
            (3, "Alex", "Hearn", "1286782.17"),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        age=A.amount(A.column("my_age")),
        null_col=A.amount(AutoMapperDataTypeLiteral(None)),
    )

    debug_text: str = mapper.to_debug_string()
    print(debug_text)

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]) == str(
        col("b.my_age").cast("double").alias("age")
    )
    assert str(sql_expressions["null_col"]) == str(
        lit(None).cast("double").alias("null_col")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert approx(
        result_df.where("member_id == 1").select("age", "null_col").collect()[0][:]
    ) == (approx(54.45), None)
    assert approx(
        result_df.where("member_id == 2").select("age", "null_col").collect()[0][:]
    ) == (approx(67.67), None)
    # Ensuring exact match in situations in which float arithmetic errors might occur
    assert (
        str(result_df.where("member_id == 3").select("age").collect()[0][0])
        == "1286782.17"
    )

    assert dict(result_df.dtypes)["age"] == "double"
    assert dict(result_df.dtypes)["null_col"] == "double"
