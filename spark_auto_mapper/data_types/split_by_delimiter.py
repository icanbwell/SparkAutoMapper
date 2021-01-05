from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import split

from spark_auto_mapper.data_types.array_base import AutoMapperArrayBase
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType


class AutoMapperSplitByDelimiterDataType(AutoMapperArrayBase):
    """
    Returns the substring from string str before count occurrences of the delimiter.
    If count is positive, everything the left of the final delimiter (counting from left) is returned.
    If count is negative, every to the right of the final delimiter (counting from the right) is returned.
    substring_index performs a case-sensitive match when searching for delimiter.
    """
    def __init__(
        self, column: AutoMapperColumnOrColumnLikeType, delimiter: str
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.delimiter: str = delimiter if delimiter.startswith(
            "["
        ) else f"[{delimiter}]"

    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        column_spec = split(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ), self.delimiter
        )
        return column_spec
