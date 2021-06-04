from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_array_max(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, [False, None, True]),
            (2, [False, False, None]),
            (3, [None, None, None]),
            (4, []),
        ],
        ["member_id", "true_false"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        member_id=A.column("member_id"), my_column=A.array_max(A.column("true_false"))
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()
    #
    assert result_df.where("member_id == 1").select("my_column").collect()[0][0]
    assert (
        False
        if result_df.where("member_id == 2").select("my_column").collect()[0][0]
        else True
    )
    assert result_df.where("member_id == 3").select("my_column").collect()[0][0] is None
