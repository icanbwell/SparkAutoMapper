from typing import Dict, List

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


class MyObject(AutoMapperDataTypeComplexBase):
    def __init__(self, my_column: List[AutoMapperDataTypeComplexBase]):
        super().__init__(my_column=my_column)


def test_auto_mapper_split_by_delimiter_and_transform(
    spark_session: SparkSession,
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "1970-01-01"),
            (2, "Vidal|Bates", "Michael", "1970-02-02"),
        ],
        ["member_id", "last_name", "first_name", "date_of_birth"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).complex(
        MyObject(
            my_column=A.transform(
                A.split_by_delimiter(A.column("last_name"), "|"),
                A.complex(bar=A.field("_"), bar2=A.field("_")),
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # assert str(sql_expressions["my_column"]) == str(
    #     split(col("b.last_name"), "[|]", -1).alias("my_column")
    # )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        result_df.where("member_id == 1").select("my_column").collect()[0][0][0]["bar"]
        == "Qureshi"
    )

    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0][0]["bar"]
        == "Vidal"
    )
    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0][1]["bar"]
        == "Bates"
    )
