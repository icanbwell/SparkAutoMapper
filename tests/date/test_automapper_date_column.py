from datetime import date
from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col
from pyspark.sql.functions import coalesce, to_date

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_date_column(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "1970-01-01"),
            (2, "Vidal", "Michael", "12/31/2020"),
        ],
        ["member_id", "last_name", "first_name", "date_of_birth"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(birthDate=A.date(A.column("date_of_birth")))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["birthDate"]) == str(
        coalesce(
            to_date(col("b.date_of_birth"), format="y-M-d"),
            to_date(col("b.date_of_birth"), format="yyyyMMdd"),
            to_date(col("b.date_of_birth"), format="M/d/y"),
        ).alias("birthDate")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("birthDate").collect()[0][
        0
    ] == date(1970, 1, 1)
    assert result_df.where("member_id == 2").select("birthDate").collect()[0][
        0
    ] == date(2020, 12, 31)

    assert dict(result_df.dtypes)["birthDate"] == "date"
