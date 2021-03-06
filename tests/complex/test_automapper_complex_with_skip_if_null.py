from typing import Dict, Optional

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


class MyClass(AutoMapperDataTypeComplexBase):
    def __init__(
        self, id_: AutoMapperTextLikeBase, name: AutoMapperTextLikeBase,
        age: AutoMapperNumberDataType
    ) -> None:
        super().__init__(id_=id_, name=name, age=age)

    def get_schema(self, include_extension: bool) -> Optional[StructType]:
        schema: StructType = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), True),
            ]
        )
        return schema


def test_automapper_complex_with_skip_if_null(
    spark_session: SparkSession
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', 45),
            (2, 'Vidal', '', 35),
        ], ['member_id', 'last_name', 'first_name', 'my_age']
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
        skip_if_columns_null_or_empty=["first_name"]
    ).complex(
        MyClass(
            id_=A.column("member_id"),
            name=A.column("last_name"),
            age=A.number(A.column("my_age"))
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert str(sql_expressions["name"]) == str(
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""),
            lit(None)
        ).otherwise(col("b.last_name")).cast(StringType()).alias("name")
    )
    assert str(sql_expressions["age"]) == str(
        when(
            col("b.first_name").isNull() | col("b.first_name").eqNullSafe(""),
            lit(None)
        ).otherwise(col("b.my_age")).cast(IntegerType()).alias("age")
    )

    result_df.printSchema()

    result_df.show()

    assert result_df.count() == 1
    assert result_df.where("id == 1").select("name"
                                             ).collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] == "int"
