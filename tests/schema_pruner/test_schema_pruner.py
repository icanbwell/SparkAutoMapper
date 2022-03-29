from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from spark_auto_mapper.schema_pruning.schema_pruner import SchemaPruner


def test_schema_pruner() -> None:
    field: StructField = StructField(
        "foo",
        StructType(
            [
                StructField("prop1", StringType()),
                StructField("prop2", IntegerType()),
            ]
        ),
    )

    SchemaPruner.prune_schema(field=field, field_data_type=field.dataType)
