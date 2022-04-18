from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, date_format, to_timestamp, coalesce

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


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

    assert_compare_expressions(
        sql_expressions["openingTime"],
        date_format(coalesce(to_timestamp(col("b.opening_time"))), "hh:mm:ss").alias(
            "openingTime"
        ),
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


def test_auto_mapper_datetime_regex_replace_format(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "1/13/1995"),
            (2, "1/3/1995"),
            (3, "11/3/1995"),
        ],
        ["member_id", "opening_date"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        formatted_date=A.datetime(
            value=A.regex_replace(
                A.column("opening_date"), pattern=r"\b(\d)(?=/)", replacement="0$1"
            ),
            formats=["M/dd/yyyy"],
        ).to_date_format("yyyy-M-dd")
    )
    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")
    result_df: DataFrame = mapper.transform(df=source_df)

    assert (
        result_df.where("member_id == 1").select("formatted_date").collect()[0][0]
        == "1995-1-13"
    )
    assert (
        result_df.where("member_id == 2").select("formatted_date").collect()[0][0]
        == "1995-1-03"
    )
    assert (
        result_df.where("member_id == 3").select("formatted_date").collect()[0][0]
        == "1995-11-03"
    )
