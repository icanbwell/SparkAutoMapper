from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_automapper_map(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', "Y"),
            (2, 'Vidal', 'Michael', "N"),
        ], ['member_id', 'last_name', 'first_name', "has_kids"]
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        has_kids=A.
        map(A.column("has_kids"), {
            "Y": "Yes",
            "N": "No"
        }, "unknown")
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]) == str(
        when(col("b.my_age").isNull(),
             None).otherwise(col("b.my_age").cast("int")).alias("age")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age"
                                                    ).collect()[0][0] == 54
    assert result_df.where("member_id == 2").select("age"
                                                    ).collect()[0][0] is None

    assert dict(result_df.dtypes)["age"] == "int"
