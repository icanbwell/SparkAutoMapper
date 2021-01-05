import json
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
        filter("identifier",
               lambda x: x["use"] == lit("usual")).alias("filtered")
    )

    result_df.show(truncate=False)

    result_text: str = result_df.toJSON().collect()[0]
    expected_json = json.loads(
        """
        {
            "filtered": [
                {
                    "use": "usual",
                    "type": {
                      "coding": [
                        {
                          "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                          "code": "PRN"
                        }
                      ]
                    },
                    "system": "http://medstarhealth.org",
                    "value": ""
                }
            ]
        }
    """
    )
    assert json.loads(result_text) == expected_json
