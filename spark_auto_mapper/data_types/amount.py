from typing import Optional
from deprecated import deprecated

from pyspark.sql import Column, DataFrame

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAmountInputType


@deprecated(
    version="0.2.15",
    reason="Use DecimalType instead to provide a fixed precision"
)
class AutoMapperAmountDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperAmountInputType):
        super().__init__()
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if source_df is not None and isinstance(self.value, AutoMapperDataTypeColumn) \
                and dict(source_df.dtypes)[self.value.value] not in ("float", "double"):
            # parse the amount here
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ).cast("double")
            return column_spec
        else:
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            return column_spec
