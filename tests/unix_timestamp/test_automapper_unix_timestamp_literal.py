from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import from_unixtime, to_timestamp

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_datetime_column_default(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "18922"),
            (2, "Vidal", "Michael", "1609390500"),
        ],
        ["member_id", "last_name", "first_name", "ts"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        timestamp=A.unix_timestamp(A.column("ts")),
        literal_val=A.unix_timestamp("1609390500"),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["literal_val"]) == str(
        to_timestamp(
            from_unixtime("1609390500", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"
        ).alias("literal_val")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.count() == 2

    assert result_df.where("member_id == 1").select("timestamp").collect()[0][
        0
    ] == datetime(1970, 1, 1, 5, 15, 22)
    assert result_df.where("member_id == 2").select("timestamp").collect()[0][
        0
    ] == datetime(2020, 12, 31, 4, 55, 0)

    assert result_df.where("member_id == 1").select("literal_val").collect()[0][
        0
    ] == datetime(2020, 12, 31, 4, 55, 0)
    assert result_df.where("member_id == 2").select("literal_val").collect()[0][
        0
    ] == datetime(2020, 12, 31, 4, 55, 0)

    assert dict(result_df.dtypes)["timestamp"] == "timestamp"
    assert dict(result_df.dtypes)["literal_val"] == "timestamp"
