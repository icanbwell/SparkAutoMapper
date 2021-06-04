from typing import Optional, Dict, List, Union

from pyspark.sql.types import StructType, StructField, DataType

from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperWithComplex(AutoMapperContainer):
    def __init__(
        self,
        entity: AutoMapperDataTypeComplexBase,
        use_schema: bool,
        include_extension: bool,
        include_null_properties: bool,
        skip_schema_validation: List[str],
        skip_if_columns_null_or_empty: Optional[List[str]],
    ) -> None:
        super().__init__()

        # ask entity for its schema
        schema: Union[StructType, DataType, None] = entity.get_schema(
            include_extension=include_extension
        )
        column_schema: Dict[str, StructField] = {}
        if schema is not None and isinstance(schema, StructType):
            # if entity has an extension then ask the extension for its schema
            column_name: str
            mapper: AutoMapperDataTypeBase
            for column_name, mapper in entity.get_child_mappers().items():
                if column_name == "extension":
                    extension_schema: Union[StructType, DataType, None]
                    extension_schema = mapper.get_schema(
                        include_extension=include_extension
                    )
                    if extension_schema is not None:
                        if (
                            isinstance(extension_schema, StructType)
                            and len(extension_schema.fields) > 0
                        ):
                            schema = StructType(
                                [f for f in schema.fields if f.name != "extension"]
                                + [extension_schema.fields[0]]
                            )
            column_schema = (
                {f.name: f for f in schema.fields} if schema and use_schema else {}
            )

        self.generate_mappers(
            mappers_dict={
                key: value for key, value in entity.get_child_mappers().items()
            },
            column_schema=column_schema,
            include_null_properties=include_null_properties or use_schema,
            skip_schema_validation=skip_schema_validation,
            skip_if_columns_null_or_empty=skip_if_columns_null_or_empty,
        )
