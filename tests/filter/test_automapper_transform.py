import json
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from spark_auto_mapper.helpers.spark_higher_order_functions import filter, transform
from pyspark.sql.functions import lit, struct


def test_automapper_transform(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    df: DataFrame = spark_session.read.json(
        str(data_json_file), multiLine=True
    )

    df.show(truncate=False)

    df.select("identifier").show(truncate=False)

    result_df = df.select(
        transform(
            filter("identifier", lambda x: x["use"] == lit("usual")),
            lambda y: struct([y["value"].alias("bar")])
        ).alias("transformed")
    )

    result_df.show(truncate=False)

    result_text: str = result_df.select("transformed").toJSON().collect()[0]
    expected_json = json.loads(
        """
        {
            "transformed": [
                {
                    "bar": "123"
                }
            ]
        }
    """
    )
    assert json.loads(result_text) == expected_json
