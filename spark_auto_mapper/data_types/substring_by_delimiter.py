from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import substring_index

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperSubstringByDelimiterDataType(AutoMapperTextLikeBase):
    """
    Returns the substring from string str before count occurrences of the delimiter.
    If count is positive, everything the left of the final delimiter (counting from left) is returned.
    If count is negative, every to the right of the final delimiter (counting from the right) is returned.
    substring_index performs a case-sensitive match when searching for delimiter.
    """

    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        delimiter: str,
        delimiter_count: int,
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.delimiter: str = delimiter
        self.delimiter_count: int = delimiter_count

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = substring_index(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            self.delimiter,
            self.delimiter_count,
        )
        return column_spec
