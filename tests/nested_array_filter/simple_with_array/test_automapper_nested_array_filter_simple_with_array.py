from os import environ
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import filter, exists, col

from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from pyspark.sql.functions import lit


def test_automapper_nested_array_filter_simple_with_array(
    spark_session: SparkSession,
) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    environ["LOGLEVEL"] = "DEBUG"

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        age=A.nested_array_filter(
            array_field=A.column("array1"),
            inner_array_field=A.field("array2"),
            match_property="reference",
            match_value=A.text("bar"),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        filter(
            col("b.array1"),
            lambda y: exists(
                y["array2"], lambda x: x["reference"] == lit("bar").cast("string")
            ),
        ).alias("age"),
    )
    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.count() == 2
    assert result_df.select("age").collect()[0][0] == []
    assert result_df.select("age").collect()[1][0][0]["array2"][0]["reference"] == "bar"
