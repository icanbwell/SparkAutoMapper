# from typing import Dict
#
# from pyspark.sql import SparkSession, Column, DataFrame
# # noinspection PyUnresolvedReferences
# from pyspark.sql.functions import col, when
# from pyspark.sql.functions import lit
# from pyspark.sql.types import StringType
#
# from spark_auto_mapper.automappers.automapper import AutoMapper
# from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
# from tests.conftest import clean_spark_session
#
#
# def test_automapper_split_and_map(spark_session: SparkSession) -> None:
#     # Arrange
#     clean_spark_session(session=spark_session)
#     spark_session.createDataFrame(
#         [
#             (1, 'Qureshi', 'Imran', "English|French"),
#             (2, 'Vidal', 'Michael', "English|Spanish"),
#         ], ['member_id', 'last_name', 'first_name', "my_language"]
#     ).createOrReplaceTempView("patients")
#
#     source_df: DataFrame = spark_session.table("patients")
#     source_df.show()
#
#     df = source_df.select("member_id")
#     df.createOrReplaceTempView("members")
#
#     # Act
#     mapper = AutoMapper(
#         view="members",
#         source_view="patients",
#         keys=["member_id"],
#         drop_key_columns=False
#     ).columns(
#         language=A.split_and_map(
#             A.column("my_language"),
#             "|",
#             lambda x: A.struct({"lang": x})
#         )
#     )
#
#     assert isinstance(mapper, AutoMapper)
#     sql_expressions: Dict[str, Column] = mapper.get_column_specs(
#         source_df=source_df
#     )
#     for column_name, sql_expression in sql_expressions.items():
#         print(f"{column_name}: {sql_expression}")
#
#     assert str(sql_expressions["age"]) == str(
#         when(
#             col("b.my_age").isNull() | col("b.my_age").eqNullSafe(""),
#             lit("100").cast(StringType())
#         ).otherwise(col("b.my_age")).alias("age")
#     )
#
#     result_df: DataFrame = mapper.transform(df=df)
#
#     # Assert
#     result_df.printSchema()
#     result_df.show()
#
#     assert result_df.where("member_id == 1").select("age"
#                                                     ).collect()[0][0] == "54"
#     assert result_df.where("member_id == 2").select("age"
#                                                     ).collect()[0][0] == "100"
#     assert result_df.where("member_id == 3").select("age"
#                                                     ).collect()[0][0] == "100"
#
#     assert dict(result_df.dtypes)["age"] == "string"
