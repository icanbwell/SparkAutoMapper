from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from tests.conftest import clean_spark_session


def test_auto_mapper_handles_duplicates(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(session=spark_session)
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Qureshi', 'Imran'),
            (3, 'Qureshi', 'Imran2'),
            (4, 'Vidal', 'Michael'),
        ], ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        dst1="src1", dst2=A.column("last_name"), dst3=A.column("first_name")
    )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    mapper.transform(df=source_df)
    result_df: DataFrame = spark_session.table("members")

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.count() == 3
