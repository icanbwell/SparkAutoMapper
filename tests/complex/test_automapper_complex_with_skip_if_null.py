from typing import Dict, Optional, Union, List

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when, lit, size
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DataType,
    IntegerType,
    ArrayType,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


class MyClass(AutoMapperDataTypeComplexBase):
    def __init__(
        self,
        id_: AutoMapperTextLikeBase,
        name: AutoMapperTextLikeBase,
        age: AutoMapperNumberDataType,
    ) -> None:
        super().__init__(id_=id_, name=name, age=age)

    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        schema: StructType = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", LongType(), True),
            ]
        )
        return schema


def test_automapper_complex_with_skip_if_null(spark_session: SparkSession) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45),
            (2, "Vidal", "", 35),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=True,
        skip_if_columns_null_or_empty=["first_name"],
    ).complex(
        MyClass(
            id_=A.column("member_id"),
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .otherwise(col("b.last_name"))
        .cast(StringType())
        .alias("name"),
    )
    assert_compare_expressions(
        sql_expressions["age"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .otherwise(col("b.my_age"))
        .cast(LongType())
        .alias("age"),
    )

    result_df.printSchema()

    result_df.show()

    assert result_df.count() == 1
    assert result_df.where("id == 1").select("name").collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")

    # Case when multiple columns are present in skip_if_columns_null_or_empty
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=True,
        skip_if_columns_null_or_empty=["first_name", "last_name"],
    ).complex(
        MyClass(
            id_=A.column("member_id"),
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df = mapper.transform(df=df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .when(
            col("b.last_name").isNull() | col("b.last_name").eqNullSafe(""), lit(None)
        )
        .otherwise(col("b.last_name"))
        .cast(StringType())
        .alias("name"),
    )
    assert_compare_expressions(
        sql_expressions["age"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .when(
            col("b.last_name").isNull() | col("b.last_name").eqNullSafe(""), lit(None)
        )
        .otherwise(col("b.my_age"))
        .cast(LongType())
        .alias("age"),
    )

    result_df.printSchema()

    result_df.show()

    assert result_df.count() == 1
    assert result_df.where("id == 1").select("name").collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")

    # Case when list column type is given in skip_if_column_null_or_empty field
    # Arrange
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45, ["123", "456"]),
            (2, "Goel", "Shubham", 35, []),
        ],
        ["member_id", "last_name", "first_name", "my_age", "list_of_ids"],
    ).createOrReplaceTempView("patients")

    source_df = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Map
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=True,
        skip_if_columns_null_or_empty=["first_name", "list_of_ids"],
    ).complex(
        MyClass(
            id_=A.column("member_id"),
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df = mapper.transform(df=df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .when(col("b.list_of_ids").isNull() | (size("b.list_of_ids") == 0), lit(None))
        .otherwise(col("b.last_name"))
        .cast(StringType())
        .alias("name"),
    )
    assert_compare_expressions(
        sql_expressions["age"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .when(col("b.list_of_ids").isNull() | (size("b.list_of_ids") == 0), lit(None))
        .otherwise(col("b.my_age"))
        .cast(LongType())
        .alias("age"),
    )

    result_df.printSchema()

    result_df.show()

    assert result_df.count() == 1
    assert result_df.where("id == 1").select("name").collect()[0][0] == "Qureshi"

    # Case when nested columns are present in skip_if_columns_null field
    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45, {"nid": 123, "ssn": "", "lis": ["123"]}),
            (2, "Goel", "Shubham", 35, {"nid": 456, "ssn": "456", "lis": ["123"]}),
            (3, "Chawla", "Gagan", 12, {"nid": 456, "ssn": "456", "lis": []}),
        ],
        StructType(
            [
                StructField("member_id", IntegerType()),
                StructField("last_name", StringType()),
                StructField("first_name", StringType()),
                StructField("my_age", IntegerType()),
                StructField(
                    "exploded",
                    StructType(
                        [
                            StructField("nid", StringType()),
                            StructField("ssn", StringType()),
                            StructField("lis", ArrayType(StringType())),
                        ]
                    ),
                ),
            ]
        ),
    ).createOrReplaceTempView("patients")

    source_df = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Map
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=True,
        skip_if_columns_null_or_empty=["first_name", "exploded.ssn", "exploded.lis"],
    ).complex(
        MyClass(
            id_=A.column("member_id"),
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df = mapper.transform(df=df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"],
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""), lit(None)
        )
        .when(
            col("b.exploded.ssn").isNull() | col("b.exploded.ssn").eqNullSafe(""),
            lit(None),
        )
        .when(col("b.exploded.lis").isNull() | (size("b.exploded.lis") == 0), lit(None))
        .otherwise(col("b.last_name"))
        .cast(StringType())
        .alias("name"),
    )

    result_df.printSchema()

    result_df.show()

    assert result_df.count() == 1
    assert result_df.where("id == 2").select("name").collect()[0][0] == "Goel"
