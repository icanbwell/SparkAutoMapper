from typing import Optional

from pyspark.sql import Column, DataFrame

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperNumberInputType


class AutoMapperNumberDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperNumberInputType):
        super().__init__()
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value)
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if (
            source_df is not None
            and isinstance(self.value, AutoMapperDataTypeColumn)
            and dict(source_df.dtypes)[self.value.value] in ("long", "int", "bigint")
        ):
            # Don't parse to long if it's already a long
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            return column_spec
        else:
            # parse the amount here
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ).cast("long")
            return column_spec
