from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser


class AutoMapperArrayDataType(AutoMapperArrayLikeBase):
    def __init__(self, value: AutoMapperDataTypeBase):
        super().__init__()
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value)
        )

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        return array(
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
        )
