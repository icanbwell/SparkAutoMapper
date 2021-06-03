from typing import Dict, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import struct

from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)


class AutoMapperDataTypeComplex(AutoMapperDataTypeComplexBase):
    def __init__(self, **kwargs: AutoMapperAnyDataType) -> None:
        super().__init__()
        self.value: Dict[str, AutoMapperDataTypeBase] = {
            key: AutoMapperValueParser.parse_value(value)
            for key, value in kwargs.items()
        }

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        return struct(
            *[
                self.get_value(
                    value=value, source_df=source_df, current_column=current_column
                ).alias(key)
                for key, value in self.value.items()
            ]
        )
