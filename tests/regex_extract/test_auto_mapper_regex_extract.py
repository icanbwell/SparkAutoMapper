from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import regexp_extract
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_regex_replace(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', "1970-01-01"),
            (2, 'Vidal', 'Michael', "1980-02-02"),
        ], ['member_id', 'last_name', 'first_name', "date_of_birth"]
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        my_column=A.regex_extract(A.column("date_of_birth"), r"^(\d{4}).*", 1)
    )

    # Assert
    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )

    assert str(sql_expressions["my_column"]) == str(
        regexp_extract(col("b.date_of_birth"), r"^(\d{4}).*",
                       1).alias("my_column")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # noinspection SpellCheckingInspection
    assert result_df.where("member_id == 1").select("my_column"
                                                    ).collect()[0][0] == "1970"
    # noinspection SpellCheckingInspection
    assert result_df.where("member_id == 2").select("my_column"
                                                    ).collect()[0][0] == "1980"
