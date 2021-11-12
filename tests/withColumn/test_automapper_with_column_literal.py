from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit

from spark_auto_mapper.automappers.automapper import AutoMapper


def test_auto_mapper_with_column_literal(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(lname="last_name")

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["lname"]) == str(lit("last_name").alias("lname"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("lname").collect()[0][0] == "last_name"
    )
    assert (
        result_df.where("member_id == 2").select("lname").collect()[0][0] == "last_name"
    )
