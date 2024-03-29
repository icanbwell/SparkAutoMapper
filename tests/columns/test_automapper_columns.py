from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import array, lit, struct, when, coalesce
from pyspark.sql.functions import col, filter

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_auto_mapper_columns(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran"),
            (2, "Vidal", "Michael"),
        ],
        ["member_id", "last_name", "first_name"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).columns(
        dst1="src1",
        dst2=AutoMapperList(["address1"]),
        dst3=AutoMapperList(["address1", "address2"]),
        dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))]),
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # Assert
    assert len(sql_expressions) == 4
    assert_compare_expressions(sql_expressions["dst1"], lit("src1").alias("dst1"))
    assert_compare_expressions(
        sql_expressions["dst2"],
        when(
            array(lit("address1")).isNotNull(),
            filter(coalesce(array(lit("address1")), array()), lambda x: x.isNotNull()),
        ).alias("dst2"),
    )
    assert_compare_expressions(
        sql_expressions["dst3"],
        when(
            array(lit("address1"), lit("address2")).isNotNull(),
            filter(
                coalesce(array(lit("address1"), lit("address2")), array()),
                lambda x: x.isNotNull(),
            ),
        ).alias("dst3"),
    )
    assert_compare_expressions(
        sql_expressions["dst4"],
        when(
            array(
                struct(lit("usual").alias("use"), col("b.last_name").alias("family"))
            ).isNotNull(),
            filter(
                coalesce(
                    array(
                        struct(
                            lit("usual").alias("use"),
                            col("b.last_name").alias("family"),
                        )
                    ),
                    array(),
                ),
                lambda x: x.isNotNull(),
            ),
        ).alias("dst4"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert len(result_df.columns) == 5
    assert result_df.where("member_id == 1").select("dst1").collect()[0][0] == "src1"
    assert (
        result_df.where("member_id == 1").select("dst2").collect()[0][0][0]
        == "address1"
    )

    assert (
        result_df.where("member_id == 1").select("dst3").collect()[0][0][0]
        == "address1"
    )
    assert (
        result_df.where("member_id == 1").select("dst3").collect()[0][0][1]
        == "address2"
    )

    assert (
        result_df.where("member_id == 1").select("dst4").collect()[0][0][0][0]
        == "usual"
    )
    assert (
        result_df.where("member_id == 1").select("dst4").collect()[0][0][0][1]
        == "Qureshi"
    )
