from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, hash

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_hash(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', "54"),
            (2, 'Vidal', "67"),
            (3, 'Vidal', None),
            (4, None, None),
        ], ['member_id', 'last_name', "my_age"]
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    source_df = source_df.withColumn("my_age", col("my_age").cast("int"))

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(age=A.hash(A.column("my_age"), A.column("last_name")))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["age"]) == str(
        hash(col("b.my_age"), col("b.last_name")).cast("string").alias("age")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1"
                           ).select("age").collect()[0][0] == "-543157534"
    assert result_df.where("member_id == 2"
                           ).select("age").collect()[0][0] == "2048196121"
    assert result_df.where("member_id == 3"
                           ).select("age").collect()[0][0] == "-80001407"
    assert result_df.where("member_id == 4").select("age"
                                                    ).collect()[0][0] == "42"

    assert dict(result_df.dtypes)["age"] == "string"
