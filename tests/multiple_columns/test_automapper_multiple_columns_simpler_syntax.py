from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

from spark_auto_mapper.automappers.automapper import AutoMapper


def test_auto_mapper_multiple_columns_simpler_syntax(
    spark_session: SparkSession
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Vidal', 'Michael'),
        ], ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False
    ).columns(dst1="src1").columns(dst2=["address1"]).columns(
        dst3=["address1", "address2"]
    ).columns(dst4=[dict(use="usual", family="[last_name]")])

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 5
    assert result_df.where("member_id == 1").select("dst1"
                                                    ).collect()[0][0] == "src1"
    assert result_df.where("member_id == 1"
                           ).select("dst2").collect()[0][0][0] == "address1"

    assert result_df.where("member_id == 1"
                           ).select("dst3").collect()[0][0][0] == "address1"
    assert result_df.where("member_id == 1"
                           ).select("dst3").collect()[0][0][1] == "address2"

    assert result_df.where("member_id == 1"
                           ).select("dst4").collect()[0][0][0][0] == "usual"
    assert result_df.where("member_id == 1"
                           ).select("dst4").collect()[0][0][0][1] == "Qureshi"
