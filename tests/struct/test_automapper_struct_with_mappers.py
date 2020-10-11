from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import expr, struct

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_struct_with_mappers(spark_session: SparkSession):
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
        value=A.struct(
            {
                "use": "usual",
                "family": A.struct(
                    {
                        "given": "foo"
                    }
                )
            }
        )
    )

    sql_expression: Column = mapper.get_column_spec()
    print(sql_expression)

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert str(sql_expression) == str(
        struct(
            expr("usual").alias("use"),
            struct(
                expr("foo").alias("given")
            ).alias("family")
        ).alias("dst2")
    )

    result_df.printSchema()
    result_df.show()

    result = result_df.where("member_id == 1").select("dst2").collect()[0][0]
    assert result[0] == "usual"
    assert result[1][0] == "foo"
