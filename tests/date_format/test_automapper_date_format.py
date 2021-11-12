from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, date_format, to_timestamp

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_date_format(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "1970-01-01 12:30"),
            (2, "Vidal", "Michael", "1970-02-02 06:30"),
        ],
        ["member_id", "last_name", "first_name", "opening_time"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    source_df = source_df.withColumn(
        "opening_time", to_timestamp("opening_time", format="yyyy-MM-dd hh:mm")
    )

    assert dict(source_df.dtypes)["opening_time"] == "timestamp"

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        openingTime=A.datetime(A.column("opening_time")).to_date_format("hh:mm:ss")
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["openingTime"]) == str(
        date_format(col("b.opening_time"), "hh:mm:ss").alias("openingTime")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("openingTime").collect()[0][0]
        == "12:30:00"
    )
    assert (
        result_df.where("member_id == 2").select("openingTime").collect()[0][0]
        == "06:30:00"
    )

    # check type
    assert dict(result_df.dtypes)["openingTime"] == "string"
