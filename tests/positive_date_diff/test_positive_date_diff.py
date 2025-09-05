from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, to_date
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


def test_positive_date_diff_with_explicit_end_date(spark_session: SparkSession) -> None:
    """
    Test case where the end_date is explicitly provided.
    """
    schema = StructType(
        [
            StructField("member_id", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
        ]
    )

    spark_session.createDataFrame(
        [
            (1, "2025-06-10", "2025-06-01"),
            (2, "2025-07-22", "2025-07-21"),
            (3, "2025-08-01", "2025-08-01"),
        ],
        schema=schema,
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    source_df = source_df.withColumn("start_date", to_date(col("start_date")))
    source_df = source_df.withColumn("end_date", to_date(col("end_date")))

    # Drop the existing view if it exists
    if "members" in [table.name for table in spark_session.catalog.listTables()]:
        spark_session.catalog.dropTempView("members")

    df = source_df.select("member_id", "start_date", "end_date")
    df.show()
    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        date_difference=A.positive_date_diff(
            A.column("start_date"), A.column("end_date")
        )
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.show()
    results = result_df.collect()

    assert (
        results[0]["date_difference"] == 9
    ), f"Expected 9, but got {results[0]['date_difference']}"
    assert (
        results[1]["date_difference"] == 1
    ), f"Expected None, but got {results[1]['date_difference']}"
    # Zero difference (should return None) and df should be empty because no positive difference


def test_positive_date_diff_with_none_end_date(spark_session: SparkSession) -> None:
    """
    Test case where the end_date is None and defaults to the current date.
    Only rows with positive date differences should be included in the result.
    """
    # Arrange
    schema = StructType(
        [
            StructField("member_id", IntegerType(), True),
            StructField("start_date", StringType(), True),
        ]
    )

    spark_session.createDataFrame(
        [
            (1, "2026-06-01"),
            (2, "2025-07-15"),
        ],
        schema=schema,
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")
    source_df = source_df.withColumn("start_date", to_date(col("start_date")))

    if "members" in [table.name for table in spark_session.catalog.listTables()]:
        spark_session.catalog.dropTempView("members")

    df = source_df.select("member_id", "start_date")

    mapper = AutoMapper(
        view="members", source_view="patients", keys=["member_id"]
    ).columns(date_difference=A.positive_date_diff(A.column("start_date")))

    result_df: DataFrame = mapper.transform(df=df)

    filtered_result_df = result_df.filter(col("date_difference").isNotNull())

    # Assert
    filtered_result_df.show()
    results = filtered_result_df.collect()

    # Dynamically calculate the expected value for the first row
    current_date = datetime.now().strftime("%Y-%m-%d")
    expected_value_row_2 = (
        datetime.strptime("2026-06-01", "%Y-%m-%d")
        - datetime.strptime(current_date, "%Y-%m-%d")
    ).days

    # Validate the positive date difference for the second row
    assert (
        len(results) == 1
    ), "Only rows with positive date differences should be included."
    assert (
        results[0]["date_difference"] == expected_value_row_2
    ), f"Expected {expected_value_row_2}, but got {results[0]['date_difference']}"
