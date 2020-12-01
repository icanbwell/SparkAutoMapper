from typing import Optional, Dict, List

from pyspark.sql.types import StructType, StructField

from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase


class AutoMapperWithComplex(AutoMapperContainer):
    def __init__(
        self, entity: AutoMapperDataTypeComplexBase, use_schema: bool,
        include_extension: bool, include_null_properties: bool,
        skip_schema_validation: List[str],
        skip_if_columns_null_or_empty: Optional[List[str]]
    ) -> None:
        super().__init__()

        # ask entity for its schema
        schema: Optional[StructType] = entity.get_schema(
            include_extension=include_extension
        )
        column_schema: Dict[str,
                            StructField] = {f.name: f
                                            for f in schema
                                            } if schema and use_schema else {}

        self.generate_mappers(
            mappers_dict={
                key: value
                for key, value in entity.get_child_mappers().items()
            },
            column_schema=column_schema,
            include_null_properties=include_null_properties or use_schema,
            skip_schema_validation=skip_schema_validation,
            skip_if_columns_null_or_empty=skip_if_columns_null_or_empty
        )
