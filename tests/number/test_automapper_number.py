from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, lit

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_number(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", "67"),
            (3, "Old", "Methusela", "131026061001"),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        age=A.number(A.column("my_age")),
        null_field=A.number(AutoMapperDataTypeLiteral(None)),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        col("b.my_age").cast("long").alias("age"),
    )

    assert_compare_expressions(
        sql_expressions["null_field"], lit(None).cast("long").alias("null_field")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age").collect()[0][0] == 54
    assert result_df.where("member_id == 2").select("age").collect()[0][0] == 67
    assert (
        result_df.where("member_id == 3").select("age").collect()[0][0] == 131026061001
    )
    assert (
        result_df.where("member_id == 1").select("null_field").collect()[0][0] is None
    )

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")
