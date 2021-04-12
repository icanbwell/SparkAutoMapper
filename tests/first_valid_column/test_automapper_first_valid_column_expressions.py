from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit, col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_automapper_first_valid_column(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', '54'),
            (2, 'Vidal', 'Michael', '33'),
        ], ['member_id', 'last_name', 'first_name', "my_age"]
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
        drop_key_columns=False
    ).columns(
        last_name=A.column("last_name"),
        age=A.first_valid_column(
            A.number(A.expression("CAST (age AS BIGINT)")),
            A.number(A.expression("CAST (my_age AS BIGINT)")),
            lit(None),
        ),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]
               ) == str(col("my_age").cast("long").alias("age"))
    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age"
                                                    ).collect()[0][0] == 54
    assert result_df.where("member_id == 2").select("age"
                                                    ).collect()[0][0] == 33
