from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import lit, struct

from spark_auto_mapper.automapper import AutoMapper
from spark_auto_mapper.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_complex(spark_session: SparkSession):
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
    ).withColumn(
        dst_column="dst2",
        value=A.complex(
            use="usual",
            family="imran"
        )
    )

    sql_expression: Column = mapper.get_column_spec()
    print(sql_expression)

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert str(sql_expression) == str(
        struct(
            lit("usual").alias("use"),
            lit("imran").alias("family")
        ).alias("dst2")
    )

    result_df.printSchema()
    result_df.show()

    result_df.where("member_id == 1").select("dst2").show()
    result_df.where("member_id == 1").select("dst2").printSchema()

    result = result_df.where("member_id == 1").select("dst2").collect()[0][0]
    assert result[0] == "usual"
    assert result[1] == "imran"
