from pyspark.sql import SparkSession, Column, DataFrame
from spark_auto_mapper.automapper import AutoMapper
from spark_auto_mapper.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_full(spark_session_per_function: SparkSession):
    spark_session: SparkSession = spark_session_per_function
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

    # example of a variable
    client_address_variable: str = "address1"

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).withColumn(
        dst_column="dst1",
        value="src1"
    ).withColumn(
        dst_column="dst2",
        value=A.list(
            client_address_variable
        )
    ).withColumn(
        dst_column="dst3",
        value=A.list(
            [client_address_variable, "address2"]
        )
    )

    company_name: str = "Microsoft"

    if company_name == "Microsoft":
        mapper = mapper.withColumn(
            dst_column="dst4",
            value=A.list(
                value=A.complex(
                    use="usual",
                    family=A.column("last_name")
                )
            )
        )

    sql_expression: Column = mapper.get_column_spec()
    print(sql_expression)

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
