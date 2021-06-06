from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from tests.conftest import clean_spark_session


def test_auto_mapper_full_no_keys(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(session=spark_session)
    spark_session.createDataFrame(
        [
            ("Qureshi", "Imran", "Iqbal"),
            ("Vidal", "Michael", "Lweis"),
        ],
        ["last_name", "first_name", "middle_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        copy_all_unmapped_properties=True,
        copy_all_unmapped_properties_exclude=["first_name"],
    ).columns(
        last_name="last_name",
    )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    mapper.transform(df=source_df)
    result_df: DataFrame = spark_session.table("members")

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 2, list(result_df.columns)

    assert result_df.columns == ["last_name", "middle_name"]
