from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from tests.conftest import clean_spark_session


def test_automapper_flatten_with_null(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)

    source_view_name = "cascaded_list_view"
    result_view_name = "flatten_list_view"
    schema = StructType(
        [
            StructField(
                "column",
                ArrayType(elementType=ArrayType(elementType=IntegerType()))
            )
        ]
    )
    source_df = spark_session.createDataFrame(
        [([[1], [2, 3, 4], [3, 5], None], )], schema=schema
    )
    source_df.printSchema()
    source_df.createOrReplaceTempView(source_view_name)

    # Act
    mapper = AutoMapper(view=result_view_name,
                        source_view=source_view_name).columns(
                            column_flat=A.flatten(A.column("column"))
                        )

    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=source_df)

    # assert
    assert result_df.select("column_flat").collect()[0][0] == [
        1, 2, 3, 4, 3, 5
    ]
