from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import regexp_replace

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_sanitize(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (
                1,
                "MedStar NRN PMR at Good Samaritan Hosp Good Health Center",
                "Imran",
                "1970-01-01",
            ),
            (2, "Vidal", "Michael", "1970-02-02"),
        ],
        ["member_id", "last_name", "first_name", "date_of_birth"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(my_column=A.column("last_name").sanitize(replacement="."))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    not_normal_characters: str = r"[^\w\r\n\t _.,!\"'/$-]"

    assert str(sql_expressions["my_column"]) == str(
        regexp_replace(col("b.last_name"), not_normal_characters, ".").alias(
            "my_column"
        )
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show(truncate=False)

    # noinspection SpellCheckingInspection
    assert (
        result_df.where("member_id == 1").select("my_column").collect()[0][0]
        == "MedStar NRN PMR at Good Samaritan Hosp.Good Health Center"
    )
    # noinspection SpellCheckingInspection
    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0] == "Vidal"
    )
