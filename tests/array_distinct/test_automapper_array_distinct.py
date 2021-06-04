from pyspark.sql import DataFrame, SparkSession

# noinspection PyUnresolvedReferences
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    ArrayType,
    StringType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_auto_mapper_array_distinct(spark_session: SparkSession) -> None:
    schema = StructType(
        [
            StructField("member_id", dataType=IntegerType(), nullable=False),
            StructField(
                "qualification",
                dataType=ArrayType(
                    StructType(
                        [
                            StructField("code", StringType(), True),
                            StructField("display", StringType(), True),
                            StructField("system", StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    # Arrange
    spark_session.createDataFrame(
        [
            (
                1,
                [
                    {
                        "code": "MD",
                        "display": "MD",
                        "system": "http://terminology.hl7.org/ValueSet/v2-2.7-030",
                    },
                    {
                        "code": "MD",
                        "display": "MD",
                        "system": "http://terminology.hl7.org/ValueSet/v2-2.7-030",
                    },
                ],
            ),
            (
                2,
                [
                    {
                        "code": "MD",
                        "display": "MD",
                        "system": "http://terminology.hl7.org/ValueSet/v2-2.7-030",
                    },
                    {
                        "code": "PhD",
                        "display": "PhD",
                        "system": "http://terminology.hl7.org/ValueSet/v2-2.7-030",
                    },
                ],
            ),
        ],
        schema,
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        member_id=A.column("member_id"),
        my_column=A.array_distinct(A.column("qualification")),
    )

    assert isinstance(mapper, AutoMapper)

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert 1 == len(
        result_df.where("member_id == 1").select("my_column").collect()[0][0]
    )
    assert 2 == len(
        result_df.where("member_id == 2").select("my_column").collect()[0][0]
    )
