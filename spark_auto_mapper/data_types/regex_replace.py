from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import regexp_replace

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType


class AutoMapperRegExReplaceDataType(AutoMapperTextLikeBase):
    """
    Concatenates multiple strings together
    """
    def __init__(
        self, column: AutoMapperColumnOrColumnLikeType, pattern: str,
        replacement: str
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.pattern: str = pattern
        self.replacement: str = replacement

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = regexp_replace(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            pattern=self.pattern,
            replacement=self.replacement
        )
        return column_spec
