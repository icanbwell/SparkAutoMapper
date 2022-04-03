from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_automapper_first_valid_column(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", "33"),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
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
            A.number(A.expression("CAST (age AS BIGINT)")),
            A.number(A.expression("CAST (my_age AS BIGINT)")),
            A.text(None),
        ),
        is_young=A.first_valid_column(
            A.map(
                A.column("age"),
                {"21": "yes", "33": "yes", "54": "no comment", None: "not provided"},
            ),
            A.map(
                A.column("my_age"),
                {"21": "yes", "33": "yes", "54": "no comment", None: "not provided"},
            ),
        ),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"], col("my_age").cast("long").cast("long").alias("age")
    )
    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age", "is_young").collect()[0][
        :
    ] == (54, "no comment")
    assert result_df.where("member_id == 2").select("age", "is_young").collect()[0][
        :
    ] == (33, "yes")
