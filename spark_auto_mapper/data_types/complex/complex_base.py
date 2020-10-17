from typing import Dict, Any

from pyspark.sql import Column
from pyspark.sql.functions import struct
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeComplexBase(AutoMapperDataTypeBase):
    def __init__(self,
                 **kwargs: Any
                 ) -> None:
        super().__init__()
        self.value: Dict[str, AutoMapperDataTypeBase] = {
            parameter_name if parameter_name != "id_" else "id": AutoMapperValueParser.parse_value(parameter_value)
            for parameter_name, parameter_value
            in kwargs.items()
            if parameter_value is not None
        }

    def get_column_spec(self) -> Column:
        return struct(
            [
                self.get_value(value).alias(key)
                for key, value in self.value.items()
            ]
        )
