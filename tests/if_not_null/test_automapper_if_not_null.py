from typing import Dict, Optional, Union, List

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when, struct
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StringType,
    LongType,
    StructField,
    StructType,
    DataType,
    IntegerType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_automapper_if_not_null(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", None),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(
        age=A.if_not_null(
            A.column("my_age"), A.number(A.column("my_age")), A.number(A.text("100"))
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        when(col("b.my_age").isNull(), lit("100").cast(StringType()).cast(LongType()))
        .otherwise(col("b.my_age").cast(LongType()))
        .alias("age"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("age").collect()[0][0] == 54
    assert result_df.where("member_id == 2").select("age").collect()[0][0] == 100

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")


def test_automapper_if_not_null_complex(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", "54"),
            (2, "Vidal", "Michael", None),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    class TestFoo(AutoMapperDataTypeComplexBase):
        def __init__(
            self,
            id: Optional[AutoMapperNumberDataType] = None,
            first_name: Optional[AutoMapperTextLikeBase] = None,
            age: Optional[AutoMapperNumberDataType] = None,
        ):
            super().__init__(id=id, first_name=first_name, age=age)

        def get_schema(
            self, include_extension: bool, extension_fields: Optional[List[str]] = None
        ) -> Optional[Union[StructType, DataType]]:
            return StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            )

    class TestComplexType(AutoMapperDataTypeComplexBase):
        def __int__(self, foo: TestFoo) -> None:
            super().__init__(foo=foo)

        def get_schema(
            self, include_extension: bool, extension_fields: Optional[List[str]] = None
        ) -> Optional[Union[StructType, DataType]]:
            return StructType(
                [
                    StructField(
                        "foo", TestFoo().get_schema(include_extension=False), False  # type: ignore
                    )
                ]
            )

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        include_null_properties=True,
    ).complex(
        TestComplexType(
            foo=A.if_not_null(
                check=A.column("my_age"),
                value=TestFoo(
                    id=A.number(A.column("member_id")),
                    first_name=A.column("first_name"),
                    age=A.number(A.column("my_age")),
                ),
                when_null=TestFoo(
                    id=A.number(A.column("member_id")), age=A.number(A.text("100"))
                ),
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["foo"],
        when(
            col("b.my_age").isNull(),
            struct(
                col("b.member_id").alias("id"),
                lit(None).alias("first_name"),
                lit("100").cast(StringType()).cast(LongType()).alias("age"),
            ),
        )
        .otherwise(
            struct(
                col("b.member_id").alias("id"),
                col("b.first_name").alias("first_name"),
                col("b.my_age").cast(LongType()).alias("age"),
            )
        )
        .alias("foo"),
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("foo.id == 1").select("foo.age").collect()[0][0] == 54
    assert result_df.where("foo.id == 2").select("foo.age").collect()[0][0] == 100
    assert (
        result_df.where("foo.id == 2").select("foo.first_name").collect()[0][0] is None
    )
