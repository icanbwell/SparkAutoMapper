from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column
from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

# noinspection PyUnresolvedReferences


def test_automapper_field(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        age=A.column("identifier").select_one(A.field("type.coding[0].code"))
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # assert str(sql_expressions["age"]
    #            ) == str(col("b.identifier[0]").alias("age"))
    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.show(truncate=False)

    assert result_df.select("age").collect()[0][0] == "PRN"
