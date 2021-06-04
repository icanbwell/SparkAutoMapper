from typing import Dict

from pyspark.sql.functions import lpad, col

from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

from pyspark.sql import SparkSession, DataFrame, Column

from spark_auto_mapper.automappers.automapper import AutoMapper


def test_auto_mapper_lpad(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "1234"),
            (2, "1234567"),
            (3, "123456789"),
        ],
        ["member_id", "empi"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(my_column=A.lpad(column=A.column("empi"), length=9, pad="0"))

    # Assert
    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)

    assert str(sql_expressions["my_column"]) == str(
        lpad(col=col("b.empi"), len=9, pad="0").alias("my_column")
    )

    result_df: DataFrame = mapper.transform(df=df)

    # noinspection SpellCheckingInspection
    assert (
        result_df.where("member_id == 1").select("my_column").collect()[0][0]
        == "000001234"
    )
    # noinspection SpellCheckingInspection
    assert (
        result_df.where("member_id == 2").select("my_column").collect()[0][0]
        == "001234567"
    )

    # noinspection SpellCheckingInspection
    assert (
        result_df.where("member_id == 3").select("my_column").collect()[0][0]
        == "123456789"
    )
