from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import substring_index
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_substring_by_delimiter(
    spark_session: SparkSession
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', "1970-01-01"),
            (2, 'Vidal', 'Michael', "1970-02-02"),
        ], ['member_id', 'last_name', 'first_name', "date_of_birth"]
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        my_column=A.substring_by_delimiter(A.column("last_name"), "s", 1)
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert str(sql_expressions["my_column"]) == str(
        substring_index(col("b.last_name"), "s", 1).alias("my_column")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("my_column"
                                                    ).collect()[0][0] == "Qure"
    assert result_df.where("member_id == 2"
                           ).select("my_column").collect()[0][0] == "Vidal"
