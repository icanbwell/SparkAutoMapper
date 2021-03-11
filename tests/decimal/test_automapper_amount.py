from decimal import Decimal
from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_amount(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', "54.45"),
            (2, 'Vidal', 'Michael', "123467.678"),
            (3, 'Paul', 'Kyle', "13"),
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
        drop_key_columns=False,
    ).columns(age=A.decimal(A.column("my_age"), 10, 2))

    debug_text: str = mapper.to_debug_string()
    print(debug_text)

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]
               ) == str(col("b.my_age").cast("decimal(10,2)").alias("age"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1"
                           ).select("age").collect()[0][0] == Decimal("54.45")
    assert result_df.where("member_id == 2").select("age").collect(
    )[0][0] == Decimal("123467.68")
    assert result_df.where("member_id == 3"
                           ).select("age").collect()[0][0] == Decimal("13.00")

    assert dict(result_df.dtypes)["age"] == "decimal(10,2)"
