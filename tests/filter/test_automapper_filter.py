from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from spark_auto_mapper.helpers.spark_higher_order_functions import filter
from pyspark.sql.functions import lit


def test_automapper_filter(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    df: DataFrame = spark_session.read.json(
        str(data_json_file), multiLine=True
    )

    df.show(truncate=False)

    df.select("identifier").show(truncate=False)

    result_df = df.select(
        filter("identifier", lambda x: x["use"] == lit("usual"))
    )

    result_df.show(truncate=False)
