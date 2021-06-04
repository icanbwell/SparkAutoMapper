from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import date_format

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperAnyDataType


class AutoMapperFormatDateTimeDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperAnyDataType, format_: str) -> None:
        """
        Converts the date or timestamp to a string in Spark


        :param value: value
        :param format_: (Optional) formats to use for trying to format the value otherwise uses Spark defaults
        """
        super().__init__()

        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value)
        )
        self.format_: str = format_

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        return date_format(
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            format=self.format_,
        )
