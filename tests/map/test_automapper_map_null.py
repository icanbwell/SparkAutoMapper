from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when, lit

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_automapper_map(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "Y"),
            (2, "Vidal", "Michael", "N"),
            (3, "Vidal", "Michael", "f"),
            (4, "Qureshi", "Imran", None),
        ],
        ["member_id", "last_name", "first_name", "has_kids"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        has_kids=A.map(
            A.column("has_kids"),
            {None: "Unspecified", "Y": "Yes", "N": "No"},
            "unknown",
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["has_kids"],
        when(col("b.has_kids").eqNullSafe(lit(None)), lit("Unspecified"))
        .when(col("b.has_kids").eqNullSafe(lit("Y")), lit("Yes"))
        .when(col("b.has_kids").eqNullSafe(lit("N")), lit("No"))
        .otherwise(lit("unknown"))
        .alias("___has_kids"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("has_kids").collect()[0][0] == "Yes"
    assert result_df.where("member_id == 2").select("has_kids").collect()[0][0] == "No"
    assert (
        result_df.where("member_id == 3").select("has_kids").collect()[0][0]
        == "unknown"
    )
    assert (
        result_df.where("member_id == 4").select("has_kids").collect()[0][0]
        == "Unspecified"
    )
