from pyspark.sql.types import StructType, StructField, DataType, ArrayType


class SchemaPruner:
    @staticmethod
    def _prune_schema_array(*, field: StructField, field_data_type: DataType) -> None:
        assert isinstance(field_data_type, ArrayType)

        SchemaPruner.prune_schema(
            field=field, field_data_type=field_data_type.elementType
        )

    @staticmethod
    def _prune_schema_struct(*, field: StructField, field_data_type: DataType) -> None:
        assert isinstance(field_data_type, StructType)

        # remove any fields that don't have the "used" field
        field_data_type.fields = [
            f for f in field_data_type.fields if hasattr(f, "used")
        ]
        field_data_type.names = [
            n
            for n in field_data_type.names
            if n in [f.name for f in field_data_type.fields]
        ]
        # remove the used tag
        for f in field_data_type.fields:
            SchemaPruner.prune_schema(field=f, field_data_type=field_data_type)
            delattr(f, "used")

    @staticmethod
    def prune_schema(*, field: StructField, field_data_type: DataType) -> None:
        if isinstance(field.dataType, StructType):
            SchemaPruner._prune_schema_struct(
                field=field, field_data_type=field_data_type
            )

        if isinstance(field.dataType, ArrayType):
            SchemaPruner._prune_schema_array(
                field=field, field_data_type=field_data_type
            )
