from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_array_multiple_items_structs(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, None, "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df: DataFrame = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        dst2=AutoMapperList(
            [
                AutoMapperDataTypeComplexBase(
                    a=A.column("first_name"), b=A.column("last_name")
                ),
                AutoMapperDataTypeComplexBase(a=A.column("first_name"), b=None),
            ],
            include_null_properties=True,
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # assert str(sql_expressions["dst2"]) == str(
    #     filter(
    #         array(lit("address1"), lit("address2")), lambda x: x.isNotNull()
    #     ).alias("dst2")
    # )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][0]
        == "Imran"
    )
    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0][1]
        == "Qureshi"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][0]
        == "Michael"
    )
    assert (
        result_df.where("member_id == 2").select("dst2").collect()[0][0][0][1] is None
    )
