from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array_join

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperJoinUsingDelimiterDataType(AutoMapperTextLikeBase):
    """
    Joins array and forms a string using the given delimiter
    """

    def __init__(self, column: AutoMapperColumnOrColumnLikeType, delimiter: str):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.delimiter: str = delimiter

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        column_spec = array_join(
            self.column.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ),
            self.delimiter,
        )
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.column
