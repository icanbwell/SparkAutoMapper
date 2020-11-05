from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit, when

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperAnyDataType


class AutoMapperIfNotNullDataType(AutoMapperTextLikeBase):
    """
    If check returns null then return null else return value
    """
    def __init__(
        self, check: AutoMapperDataTypeColumn, value: AutoMapperAnyDataType
    ):
        super().__init__()

        self.check: AutoMapperDataTypeColumn = check
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = when(
            self.check.get_column_spec(source_df=source_df).isNull(),
            lit(None)
        ).otherwise(self.value.get_column_spec(source_df=source_df))

        return column_spec
