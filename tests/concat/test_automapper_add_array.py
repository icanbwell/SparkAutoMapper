from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column

from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions
from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from pyspark.sql.functions import lit, concat, array, col


def test_automapper_add_array(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    # Act
    mapper = AutoMapper(
        view="members", source_view="patients", drop_key_columns=False
    ).columns(age=A.column("identifier") + A.text("foo").to_array())

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["age"],
        concat(col("b.identifier"), array(lit("foo").cast("string"))).alias("age"),
    )
    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.show(truncate=False)

    assert result_df.where("id == 1730325416").select("age").collect()[0][0] == [
        "bar",
        "foo",
    ]

    assert result_df.where("id == 1467734301").select("age").collect()[0][0] == [
        "John",
        "foo",
    ]
