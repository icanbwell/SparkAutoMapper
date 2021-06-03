from typing import Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import lpad

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperLPadDataType(AutoMapperTextLikeBase):
    """
    Returns column value, left-padded with pad to a length of length. If column value is longer than length,
    the return value is shortened to length characters.
    """

    def __init__(self, column: AutoMapperColumnOrColumnLikeType, length: int, pad: str):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.length: int = length
        self.pad: str = pad

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = lpad(
            col=self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            len=self.length,
            pad=self.pad,
        )
        return column_spec
