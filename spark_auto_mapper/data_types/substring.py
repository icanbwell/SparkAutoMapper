from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import substring

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperSubstringDataType(AutoMapperTextLikeBase):
    """
    Finds a substring in a string
    """

    def __init__(
        self, column: AutoMapperColumnOrColumnLikeType, start: int, length: int
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.start: int = start
        self.length: int = length

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = substring(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            self.start,
            self.length,
        )
        return column_spec
