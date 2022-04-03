from typing import Dict, Optional, Union, List

# noinspection PyPackageRequirements
from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyPackageRequirements
from pyspark.sql.functions import col, array

# noinspection PyPackageRequirements
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DataType,
    ArrayType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


class MyClass(AutoMapperDataTypeComplexBase):
    def __init__(
        self,
        name: AutoMapperTextLikeBase,
        age: AutoMapperNumberDataType,
        my_array: Optional[AutoMapperList[AutoMapperNumberDataType]],
    ) -> None:
        super().__init__(name=name, age=age, my_array=my_array)

    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        schema: StructType = StructType(
            [
                StructField("name", StringType(), False),
                StructField("age", LongType(), True),
                StructField("my_array", ArrayType(LongType()), True),
            ]
        )
        return schema


def test_auto_mapper_cast(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45),
            (2, "Vidal", "Michael", 35),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    source_df = source_df.withColumn("an_array", array())
    source_df.createOrReplaceTempView("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False,
    ).complex(
        MyClass(
            name=A.column("last_name"),
            age=A.column("my_age").cast(AutoMapperNumberDataType),
            my_array=A.column("an_array").cast(
                AutoMapperList[AutoMapperNumberDataType]
            ),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"], col("b.last_name").cast("string").alias("name")
    )
    assert_compare_expressions(
        sql_expressions["age"], col("b.my_age").cast("long").alias("age")
    )

    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("name").collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")
