from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import substring

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase


class AutoMapperSubstringDataType(AutoMapperTextLikeBase):
    """
    Finds a substring in a string
    """
    def __init__(
        self, column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        start: int, length: int
    ):
        super().__init__()

        self.column: Union[AutoMapperDataTypeColumn,
                           AutoMapperTextLikeBase] = column
        self.start: int = start
        self.length: int = length

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = substring(
            self.column.get_column_spec(source_df=source_df), self.start,
            self.length
        )
        return column_spec
