from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_full_no_views(spark_session: SparkSession) -> None:
    # Arrange
    source_df = spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    )

    # example of a variable
    client_address_variable: str = "address1"

    # Act
    mapper = AutoMapper(keys=["member_id"], drop_key_columns=False).columns(
        dst1="src1",
        dst2=AutoMapperList([client_address_variable]),
        dst3=AutoMapperList([client_address_variable, "address2"]),
    )

    company_name: str = "Microsoft"

    if company_name == "Microsoft":
        mapper = mapper.columns(
            dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))])
        )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 5
    assert result_df.where("member_id == 1").select("dst1").collect()[0][0] == "src1"
    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0]
        == "address1"
    )

    assert (
        result_df.where("member_id == 1").select("dst3").collect()[0][0][0]
        == "address1"
    )
    assert (
        result_df.where("member_id == 1").select("dst3").collect()[0][0][1]
        == "address2"
    )

    assert (
        result_df.where("member_id == 1").select("dst4").collect()[0][0][0][0]
        == "usual"
    )
    assert (
        result_df.where("member_id == 1").select("dst4").collect()[0][0][0][1]
        == "Qureshi"
    )
