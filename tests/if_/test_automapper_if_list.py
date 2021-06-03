from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, LongType

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_automapper_if_list(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Qureshi", "Imran", "59"),
            (3, "Vidal", "Michael", None),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        age=A.if_(
            column=A.column("my_age"),
            check=["54", "59"],
            value=A.number(A.column("my_age")),
            else_=A.number(A.text("100")),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]) == str(
        when(col("b.my_age").isin(["54", "59"]), col("b.my_age").cast("long"))
        .otherwise(lit("100").cast(StringType()).cast(LongType()))
        .alias("age")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age").collect()[0][0] == 54
    assert result_df.where("member_id == 2").select("age").collect()[0][0] == 59
    assert result_df.where("member_id == 3").select("age").collect()[0][0] == 100
    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")
