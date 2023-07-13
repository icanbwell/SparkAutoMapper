from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from tests.conftest import clean_spark_session

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

from tests.cross_column_filter.location import AutoMapperElasticSearchLocation


def test_cross_column_lookup_filter(spark_session: SparkSession) -> None:
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")

    data_json_file: Path = data_dir.joinpath("data.json")

    source_df: DataFrame = spark_session.read.json(str(data_json_file), multiLine=True)

    source_df.createOrReplaceTempView("orgs")

    source_df.show(truncate=False)

    mapper = AutoMapper(view="dest", source_view="orgs").columns(
        location=A.column("location").select(
            AutoMapperElasticSearchLocation(
                organization_name=A.cross_column_filter(
                    array_field=A.column("organization"),
                    match_property="id",
                    match_value=A.field("{parent}.managingOrganization.reference"),
                ).select_one(A.field("name"))
            )
        )
    )

    # Assert
    print("------RESULTS------")

    result_df: DataFrame = mapper.transform(df=source_df)

    result_df.printSchema()
    result_df.show(truncate=False)

    first_row = result_df.collect()[0].location
    assert first_row[0].organization_name == "Organization 3"

    second_row = result_df.collect()[1].location
    assert second_row[0].organization_name == "Organization 2"
