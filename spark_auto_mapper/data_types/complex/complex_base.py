from typing import Dict, Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import struct
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeComplexBase(AutoMapperDataTypeBase):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self.value: Dict[str, AutoMapperDataTypeBase] = {
            parameter_name if not parameter_name.endswith(
                "_"
            )  # some property names are python keywords so we have to append with _
            else parameter_name[:-1]:
            AutoMapperValueParser.parse_value(parameter_value)
            for parameter_name, parameter_value in kwargs.items()
            if parameter_value is not None
        }

    def get_column_spec(self, source_df: DataFrame) -> Column:
        return struct(
            [
                self.get_value(value=value, source_df=source_df).alias(key)
                for key, value in self.value.items()
            ]
        )
