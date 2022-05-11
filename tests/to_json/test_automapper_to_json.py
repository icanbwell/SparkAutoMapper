from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, to_json
from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.helpers.expression_comparer import assert_compare_expressions


def test_automapper_to_json(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    df.createOrReplaceTempView("patients")

    df.show(truncate=False)

    # Act
    mapper = AutoMapper(view="members", source_view="patients").columns(
        member_id=A.column("id"), my_json=A.column("identifier").to_json()
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(source_df=df)
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    assert_compare_expressions(
        sql_expressions["my_json"],
        to_json(col("b.identifier")).alias("my_json"),
        ignore_casts=False,
    )

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert (
        (result_df.where("member_id == '1730325416'").select("my_json").collect()[0][0])
        == '[{"system":"http://medstarhealth.org","type":{"coding":[{"code":"PRN","system":"http://terminology.hl7.org/CodeSystem/v2-0203"}]},"use":"usual","value":"123"},{"system":"http://hl7.org/fhir/sid/us-npi","type":{"coding":[{"code":"NPI","system":"http://terminology.hl7.org/CodeSystem/v2-0203"}]},"use":"official","value":"1730325416"}]'
    )
    assert (
        (result_df.where("member_id == 1467734301").select("my_json").collect()[0][0])
        == '[{"system":"http://medstarhealth.org","type":{"coding":[{"code":"PRN","system":"http://terminology.hl7.org/CodeSystem/v2-0203"}]},"use":"usual","value":"104089"},{"system":"http://hl7.org/fhir/sid/us-npi","type":{"coding":[{"code":"NPI","system":"http://terminology.hl7.org/CodeSystem/v2-0203"}]},"use":"official","value":"1467734301"}]'
    )

    assert dict(result_df.dtypes)["my_json"] == "string"
