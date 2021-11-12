from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_automapper_first_valid_column(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", "33"),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df_1: DataFrame = spark_session.table("patients")

    df = source_df_1.select("member_id")
    df.createOrReplaceTempView("members")

    # The key thing in this test is that we are using the same mapper on sources with different columns, and they both
    # work as expected.

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        last_name=A.column("last_name"),
        age=A.first_valid_column(
            A.number(A.column("age")),
            A.number(A.column("my_age")),
            A.text(None),
        ),
        age2=A.first_valid_column(
            A.if_not_null(
                A.first_valid_column(
                    A.number(A.column("age")),
                    A.number(A.column("my_age")),
                    A.text(None),
                ),
                A.first_valid_column(
                    A.number(A.column("age")),
                    A.number(A.column("my_age")),
                    A.text(None),
                ),
                A.number(A.text("100")),
            ),
            A.number(A.column("age")),
            A.number(A.column("his_age")),
            A.number(99999),
        ),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions_1: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df_1
    )
    for column_name, sql_expression in sql_expressions_1.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions_1["age"]) == str(
        col("b.my_age").cast("long").alias("age")
    )
    result_df_1: DataFrame = mapper.transform(df=df)

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", None),
        ],
        ["member_id", "last_name", "first_name", "age"],
    ).createOrReplaceTempView("patients")

    source_df_2 = spark_session.table("patients")

    df = source_df_1.select("member_id")
    df.createOrReplaceTempView("members")

    sql_expressions_2: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df_2
    )
    assert str(sql_expressions_2["age"]) == str(
        col("b.age").cast("long").alias("___age")
    )

    result_df_2 = mapper.transform(df=df)

    # Assert
    result_df_1.printSchema()
    result_df_1.show()

    result_df_2.printSchema()
    result_df_2.show()

    assert result_df_1.where("member_id == 1").select("age", "age2").collect()[0][
        :
    ] == (54, 54)
    assert result_df_1.where("member_id == 2").select("age", "age2").collect()[0][
        :
    ] == (33, 33)

    assert result_df_2.where("member_id == 1").select("age", "age2").collect()[0][
        :
    ] == (54, 54)
    assert result_df_2.where("member_id == 2").select("age", "age2").collect()[0][
        :
    ] == (None, 100)
