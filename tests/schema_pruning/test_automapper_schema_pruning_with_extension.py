from typing import Dict, Optional, Union, List

# noinspection PyPackageRequirements
from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyPackageRequirements
from pyspark.sql.functions import col

# noinspection PyPackageRequirements
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DataType,
)
from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
)

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from spark_auto_mapper.type_definitions.defined_types import AutoMapperDateInputType
from tests.conftest import clean_spark_session


class MyProcessingStatusExtensionItem(AutoMapperDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(
        self,
        url: str,
        valueString: Optional[AutoMapperTextLikeBase] = None,
        valueUrl: Optional[AutoMapperTextLikeBase] = None,
        valueDateTime: Optional[AutoMapperDateInputType] = None,
    ) -> None:
        super().__init__(
            url=url,
            valueString=valueString,
            valueUrl=valueUrl,
            valueDateTime=valueDateTime,
        )


class MyProcessingStatusExtension(AutoMapperDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(
        self,
        processing_status: AutoMapperTextLikeBase,
        request_id: AutoMapperTextLikeBase,
        date_processed: Optional[AutoMapperDateInputType] = None,
    ) -> None:
        definition_base_url = "https://raw.githubusercontent.com/imranq2/SparkAutoMapper.FHIR/main/StructureDefinition/"
        processing_status_extensions = [
            MyProcessingStatusExtensionItem(
                url="processing_status",
                valueString=processing_status,
            ),
            MyProcessingStatusExtensionItem(
                url="request_id",
                valueString=request_id,
            ),
        ]
        self.extensions = processing_status_extensions
        super().__init__(
            url=definition_base_url,
            extension=AutoMapperList(processing_status_extensions),
        )

    def include_null_properties(self, include_null_properties: bool) -> None:
        for item in self.extensions:
            item.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        return MyProcessingStatusExtension.get_schema_static(
            include_extension=include_extension
        )

    @staticmethod
    def get_schema_static(
        include_extension: bool = False,
    ) -> StructType:
        return StructType(
            [
                StructField("url", StringType()),
                StructField("extra", StringType(), True),
                StructField(
                    "extension",
                    ArrayType(
                        StructType(
                            [
                                StructField("url", StringType()),
                                StructField("valueString", StringType()),
                                StructField("valueUrl", StringType()),
                                StructField("valueDateTime", TimestampType()),
                            ]
                        )
                    ),
                ),
            ]
        )

    def get_value(
        self,
        value: AutoMapperDataTypeBase,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        return super().get_value(value, source_df, current_column, parent_columns)


class MyClass(AutoMapperDataTypeComplexBase):
    def __init__(
        self,
        name: AutoMapperTextLikeBase,
        age: AutoMapperNumberDataType,
        extension: AutoMapperList[MyProcessingStatusExtension],
    ) -> None:
        super().__init__(name=name, age=age, extension=extension)

    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        schema: StructType = StructType(
            [
                StructField("name", StringType(), False),
                StructField("extra", LongType(), True),
                StructField("age", LongType(), True),
                StructField(
                    "extension",
                    ArrayType(MyProcessingStatusExtension.get_schema_static()),
                    True,
                ),
            ]
        )
        return schema


def test_auto_mapper_schema_pruning_with_extension(
    spark_session: SparkSession,
) -> None:
    # Arrange
    clean_spark_session(spark_session)

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45),
            (2, "Vidal", "Michael", 35),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        enable_schema_pruning=True,
        skip_schema_validation=[],
    ).complex(
        MyClass(
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
            extension=AutoMapperList(
                [
                    MyProcessingStatusExtension(
                        processing_status=A.text("foo"),
                        request_id=A.text("bar"),
                        date_processed=A.date("2021-01-01"),
                    )
                ]
            ),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=source_df)

    # Assert
    assert_compare_expressions(
        sql_expressions["name"], col("b.last_name").cast("string").alias("name")
    )
    assert_compare_expressions(
        sql_expressions["age"], col("b.my_age").cast("long").alias("age")
    )

    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.where("member_id == 1").select("name").collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] in ("int", "long", "bigint")

    # confirm schema
    expected_schema: StructType = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
            StructField(
                "extension",
                ArrayType(
                    StructType(
                        [
                            StructField("url", StringType()),
                            StructField(
                                "extension",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("url", StringType()),
                                            StructField("valueString", StringType()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    result: SchemaComparerResult = SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=result_df.schema,
        desired_schema=expected_schema,
    )

    assert result.errors == [], str(result)
