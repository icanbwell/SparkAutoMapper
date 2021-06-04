from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DecimalType

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAmountInputType


class AutoMapperDecimalDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperAmountInputType, precision: int, scale: int):
        """
        Specifies the value should be used as a decimal
        :param value:
        :param precision: the maximum total number of digits (on both sides of dot)
        :param scale: the number of digits on right side of dot
        """
        super().__init__()
        self.precision = precision
        self.scale = scale
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
            and "decimal" not in dict(source_df.dtypes)[self.value.value]
        ):
            # parse the amount here
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ).cast(DecimalType(precision=self.precision, scale=self.scale))
            return column_spec
        else:
            # Already a decimal
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            return column_spec
