from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import array
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit

from spark_auto_mapper.automapper import AutoMapper
from spark_auto_mapper.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_array_multiple_items(spark_session: SparkSession):
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Vidal', 'Michael'),
        ],
        ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df: DataFrame = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).withColumn(
        dst_column="dst2",
        value=A.list(
            [
                "address1",
                "address2"
            ]
        )
    )

    sql_expression: Column = mapper.get_column_spec()
    print(sql_expression)

    assert str(sql_expression) == str(array(lit("address1"), lit("address2")).alias("dst2"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("dst2").collect()[0][0][0] == "address1"
    assert result_df.where("member_id == 1").select("dst2").collect()[0][0][1] == "address2"
    assert result_df.where("member_id == 2").select("dst2").collect()[0][0][0] == "address1"
    assert result_df.where("member_id == 2").select("dst2").collect()[0][0][1] == "address2"
