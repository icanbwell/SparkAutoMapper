from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import array_join

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_join_using_delimiter(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (123456789, "Gagan", "Chawla", ["MD", "PhD"]),
        ],
        ["npi", "first_name", "last_name", "suffix"],
    ).createOrReplaceTempView("practitioners")

    source_df: DataFrame = spark_session.table("practitioners")

    df = source_df.select("npi")
    df.createOrReplaceTempView("physicians")

    # Act
    mapper = AutoMapper(
        view="physicians", source_view="practitioners", keys=["npi"]
    ).columns(my_column=A.join_using_delimiter(A.column("suffix"), ", "))

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["my_column"]) == str(
        array_join(col("b.suffix"), ", ").alias("my_column")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()
    assert (
        result_df.where("npi == 123456789").select("my_column").collect()[0][0]
        == "MD, PhD"
    )
