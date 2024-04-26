from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import lower

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_lower(spark_session: SparkSession) -> None:
    # Arrange
    patient_1 = ["Shubham", "GOEL"]
    patient_2 = ["John", "Wick"]
    spark_session.createDataFrame(
        [
            (1, patient_1[1], patient_1[0], "1970-01-01"),
            (2, patient_2[1], patient_2[0], "1970-02-02"),
        ],
        ["member_id", "last_name", "first_name", "date_of_birth"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(my_column=A.lower(A.column("last_name")))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["my_column"],
        lower(col("b.last_name")).alias("my_column"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("my_column").collect()[0][0]
        == patient_1[1].lower()
    )
    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0]
        == patient_2[1].lower()
    )
