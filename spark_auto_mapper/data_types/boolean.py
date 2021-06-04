from typing import Optional

from pyspark.sql import Column, DataFrame

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperBooleanInputType


class AutoMapperBooleanDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperBooleanInputType):
        super().__init__()
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value)
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        # parse the boolean here
        column_spec = self.value.get_column_spec(
            source_df=source_df, current_column=current_column
        ).cast("boolean")
        return column_spec
