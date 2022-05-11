from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_automapper_cast_to_type_typed(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 54.45, True),
            (2, "Vidal", "Michael", 67.67, False),
        ],
        ["member_id", "last_name", "first_name", "my_age", "married"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    # source_df = source_df.withColumn("my_age", col("my_age").cast("float"))

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        age=A.column("my_age").to_string(), ageInYears=A.column("my_age").to_type("int")
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        col("b.my_age").cast("string").alias("age"),
        ignore_casts=False,
    )
    assert_compare_expressions(
        sql_expressions["ageInYears"],
        col("b.my_age").cast("integer").alias("ageInYears"),
        ignore_casts=False,
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (result_df.where("member_id == 1").select("age").collect()[0][0]) == "54.45"
    assert (result_df.where("member_id == 2").select("age").collect()[0][0]) == "67.67"

    assert dict(result_df.dtypes)["age"] == "string"

    assert (
        result_df.where("member_id == 1").select("ageInYears").collect()[0][0]
    ) == 54
    assert (
        result_df.where("member_id == 2").select("ageInYears").collect()[0][0]
    ) == 67

    assert dict(result_df.dtypes)["ageInYears"] == "int"
