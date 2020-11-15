from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import regexp_replace
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase


class AutoMapperRegExReplaceDataType(AutoMapperTextLikeBase):
    """
    Concatenates multiple strings together
    """
    def __init__(
        self, column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        pattern: str, replacement: str
    ):
        super().__init__()

        self.column: Union[AutoMapperDataTypeColumn,
                           AutoMapperTextLikeBase] = column
        self.pattern: str = pattern
        self.replacement: str = replacement

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = regexp_replace(
            self.column.get_column_spec(source_df=source_df),
            pattern=self.pattern,
            replacement=self.replacement
        )
        return column_spec
