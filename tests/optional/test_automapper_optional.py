from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

from spark_auto_mapper.data_types.optional import AutoMapperOptionalType


def test_automapper_optional(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', "54"),
            (2, 'Vidal', 'Michael', None),
        ], ['member_id', 'last_name', 'first_name', "my_age"]
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False
    ).columns(
        last_name=A.column("last_name"),
        optional_age=AutoMapperOptionalType(
            A.number(A.column("my_age")), A.text("my_age")
        ),
        optional_foo=AutoMapperOptionalType(A.column("foo"), A.text("foo")),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["optional_age"]
               ) == str(col("b.my_age").cast("long").alias("optional_age"))
    assert str(sql_expressions["optional_foo"]
               ) == str(lit("foo").cast("string").alias("optional_foo"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select(
        "optional_age", "optional_foo"
    ).collect()[0][:] == (54, "foo")
    assert result_df.where("member_id == 2").select(
        "optional_age", "optional_foo"
    ).collect()[0][:] == (None, "foo")
