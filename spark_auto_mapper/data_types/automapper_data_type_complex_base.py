from typing import Dict

from pyspark.sql import Column
from pyspark.sql.functions import struct

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeComplexBase(AutoMapperDataTypeBase):
    def __init__(self) -> None:
        super().__init__()
        self.value: Dict[str, AutoMapperDataTypeBase] = {}

    def get_column_spec(self) -> Column:
        return struct(
            [
                self.get_value(value).alias(key)
                for key, value in self.value.items()
            ]
        )
