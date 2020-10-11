from typing import Dict, Any

from pyspark.sql import Column
from pyspark.sql.functions import struct

from spark_auto_mapper.automapper_value_parser import AutoMapperValueParser
from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_data_type_complex_base import AutoMapperDataTypeComplexBase


class AutoMapperDataTypeStruct(AutoMapperDataTypeComplexBase):
    def __init__(self, value: Dict[str, Any]):
        super().__init__()
        assert isinstance(value, dict)
        self.value: Dict[str, AutoMapperDataTypeBase] = {
            key: AutoMapperValueParser.parse_value(value) for key, value in value.items()
        }

    def get_column_spec(self) -> Column:
        return struct(
            [
                self.get_value(value).alias(key)
                for key, value in self.value.items()
            ]
        )
