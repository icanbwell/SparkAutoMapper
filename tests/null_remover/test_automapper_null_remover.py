from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Column

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from tests.conftest import clean_spark_session


def test_automapper_null_remover(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("patients")

    source_df.show(truncate=False)

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        address=A.if_not_null(
            A.column("address"),
            value=A.column("address").select(
                A.if_not_null(
                    A.field("line"),
                    A.field("line")
                    .select(A.current().sanitize())
                    .remove_null_or_empty(),
                )
            ),
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=source_df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    # assert str(sql_expressions["age"]) == str(
    #     filter("b.identifier", lambda x: x["use"] == lit("usual")).alias("age")
    # )
    result_df: DataFrame = mapper.transform(df=source_df)

    print(result_df.select("address").collect()[0][0])
    assert result_df.select("address").collect()[0][0][0] == [
        "1111 STREET LN",
        "SUITE 256",
    ]
    result_df.show(truncate=False)
