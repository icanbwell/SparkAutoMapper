from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import concat

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit, col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_concat_column(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "1970-01-01"),
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
    ).columns(my_column=A.concat("pre-", A.column("last_name"), "-post"))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["my_column"],
        concat(lit("pre-"), col("b.last_name"), lit("-post")).alias("my_column"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("my_column").collect()[0][0]
        == "pre-Qureshi-post"
    )
    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0]
        == "pre-Vidal-post"
    )
