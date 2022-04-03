from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import array, expr, filter, when, coalesce

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_array_single_item(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
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
    ).columns(dst2=AutoMapperList(["address1"]))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["dst2"],
        when(
            array(expr("address1")).isNotNull(),
            filter(coalesce(array(expr("address1")), array()), lambda x: x.isNotNull()),
        ).alias("dst2"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0]
        == "address1"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0]
        == "address1"
    )
