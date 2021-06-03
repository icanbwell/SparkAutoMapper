from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from tests.conftest import clean_spark_session


def test_auto_mapper_full_no_keys(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(session=spark_session)
    spark_session.createDataFrame(
        [
            ("Qureshi", "Imran"),
            ("Vidal", "Michael"),
        ],
        ["last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # example of a variable
    client_address_variable: str = "address1"

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        dst1="src1",
        dst2=AutoMapperList([client_address_variable]),
        dst3=AutoMapperList([client_address_variable, "address2"]),
    )

    company_name: str = "Microsoft"

    if company_name == "Microsoft":
        mapper = mapper.columns(
            dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))]),
            dst5=AutoMapperList([A.complex(use="usual", first=A.column("first_name"))]),
        )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    mapper.transform(df=source_df)
    result_df: DataFrame = spark_session.table("members")

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 5, list(result_df.columns)
    assert (
        result_df.where("dst4[0].family == 'Qureshi'").select("dst1").collect()[0][0]
        == "src1"
    )
    assert (
        result_df.where("dst4[0].family == 'Qureshi'").select("dst2").collect()[0][0][0]
        == "address1"
    )

    assert (
        result_df.where("dst4[0].family == 'Qureshi'").select("dst3").collect()[0][0][0]
        == "address1"
    )
    assert (
        result_df.where("dst4[0].family == 'Qureshi'").select("dst3").collect()[0][0][1]
        == "address2"
    )

    assert (
        result_df.where("dst4[0].family == 'Qureshi'")
        .select("dst4")
        .collect()[0][0][0][0]
        == "usual"
    )
    assert (
        result_df.where("dst4[0].family == 'Qureshi'")
        .select("dst4")
        .collect()[0][0][0][1]
        == "Qureshi"
    )
    assert (
        result_df.where("dst4[0].family == 'Qureshi'")
        .select("dst5")
        .collect()[0][0][0][1]
        == "Imran"
    )
