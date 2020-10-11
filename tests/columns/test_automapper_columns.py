from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import array, lit, struct
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_columns(spark_session: SparkSession):
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Vidal', 'Michael'),
        ],
        ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).columns(
        dst1="src1",
        dst2=A.list(
            "address1"
        ),
        dst3=A.list(
            ["address1", "address2"]
        ),
        dst4=A.list(
            A.complex(
                use="usual",
                family=A.column("last_name")
            )
        )
    )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs()
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # Assert
    assert len(sql_expressions) == 4
    assert str(sql_expressions["dst1"]) == str(lit("src1").alias("dst1"))
    assert str(sql_expressions["dst2"]) == str(array(lit("address1")).alias("dst2"))
    assert str(sql_expressions["dst3"]) == str(array(lit("address1"), lit("address2")).alias("dst3"))
    assert str(sql_expressions["dst4"]) == str(
        array(
            struct(
                lit("usual").alias("use"),
                col("last_name").alias("family")
            )
        ).alias("dst4"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 5
    assert result_df.where("member_id == 1").select("dst1").collect()[0][0] == "src1"
    assert result_df.where("member_id == 1").select("dst2").collect()[0][0][0] == "address1"

    assert result_df.where("member_id == 1").select("dst3").collect()[0][0][0] == "address1"
    assert result_df.where("member_id == 1").select("dst3").collect()[0][0][1] == "address2"

    assert result_df.where("member_id == 1").select("dst4").collect()[0][0][0][0] == "usual"
    assert result_df.where("member_id == 1").select("dst4").collect()[0][0][0][1] == "Qureshi"
