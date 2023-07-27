from pathlib import Path
from typing import Dict, Optional, List, Union

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import filter
from pyspark.sql.types import StructType, DataType, StructField, StringType

from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from spark_auto_mapper.type_definitions.defined_types import AutoMapperString
from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from pyspark.sql.functions import lit


def test_automapper_filter(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        age=A.filter(
            column=A.column("identifier"), func=lambda x: x["use"] == lit("usual")
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        filter("b.identifier", lambda x: x["use"] == lit("usual")).alias("age"),
    )
    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.show(truncate=False)


def test_automapper_filter_column_using_field_from_current(
    spark_session: SparkSession,
) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    class TestComplexType(AutoMapperDataTypeComplexBase):
        def __int__(self, foo: AutoMapperString) -> None:
            super().__init__(foo=foo)

        def get_schema(
            self, include_extension: bool, extension_fields: Optional[List[str]] = None
        ) -> Optional[Union[StructType, DataType]]:
            return StructType([StructField("foo", StringType(), False)])

    # Act
    mapper = AutoMapper(view="members", source_view="patients").complex(
        TestComplexType(
            foo=A.column("name").select(
                A.column("identifier")
                .filter(lambda x: x["use"] == A.text("usual"))
                .select_one(A.field("value"))
            )
        )
    )

    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.show(truncate=False)
