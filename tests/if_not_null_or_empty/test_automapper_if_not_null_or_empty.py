from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from tests.conftest import clean_spark_session


def test_automapper_if_not_null_or_empty(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(session=spark_session)
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", ""),
            (3, "Vidal3", "Michael", None),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    source_df.show()

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        age=A.if_not_null_or_empty(
            A.column("my_age"), A.column("my_age"), A.text("100")
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        when(
            col("b.my_age").isNull() | col("b.my_age").eqNullSafe(""),
            lit("100").cast(StringType()),
        )
        .otherwise(col("b.my_age"))
        .alias("age"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age").collect()[0][0] == "54"
    assert result_df.where("member_id == 2").select("age").collect()[0][0] == "100"
    assert result_df.where("member_id == 3").select("age").collect()[0][0] == "100"

    assert dict(result_df.dtypes)["age"] == "string"
