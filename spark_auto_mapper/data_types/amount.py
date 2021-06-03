from typing import Optional
from deprecated import deprecated

from pyspark.sql import Column, DataFrame

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAmountInputType


@deprecated(
    version="0.2.15", reason="Use DecimalType instead to provide a fixed precision"
)
class AutoMapperAmountDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperAmountInputType):
        super().__init__()
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value)
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = self.value.get_column_spec(
            source_df=source_df, current_column=current_column
        )
        if not (
            isinstance(self.value, AutoMapperDataTypeColumn)
            and source_df
            and dict(source_df.dtypes)[self.value.value] in ("float", "double")
        ):
            column_spec = column_spec.cast("double")
        return column_spec
