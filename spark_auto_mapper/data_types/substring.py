from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import substring

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
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
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        column_spec = substring(
            self.column.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ),
            self.start,
            self.length,
        )
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.column
