from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import regexp_extract

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType


class AutoMapperRegExExtractDataType(AutoMapperTextLikeBase):
    """
    Extracts a regex from a column. Will return an empty string if no match is found.

    Note that regexp_extract requires that the pattern match the *entire* string - it does
    the equivalent of a python re.match, not re.search
    """
    def __init__(
        self, column: AutoMapperColumnOrColumnLikeType, pattern: str,
        index: int
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.pattern: str = pattern
        self.index: int = index

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = regexp_extract(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            pattern=self.pattern,
            idx=self.index
        )
        return column_spec
