from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.if_column_exists import AutoMapperIfColumnExistsType
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_automapper_optional_ifexists(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", None),
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
        optional_age=AutoMapperIfColumnExistsType(
            column=A.column("my_age"),
            if_exists=A.number(A.column("my_age")),
            if_not_exists=A.text("no age"),
        ),
        optional_foo=AutoMapperIfColumnExistsType(
            column=A.column("foo"),
            if_exists=A.text("foo col is there"),
            if_not_exists=A.text("no foo"),
        ),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select(
        "optional_age", "optional_foo"
    ).collect()[0][:] == (54, "no foo")
    assert result_df.where("member_id == 2").select(
        "optional_age", "optional_foo"
    ).collect()[0][:] == (None, "no foo")
