from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import base64

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperBase64DataType(AutoMapperTextLikeBase):
    """
    Computes the BASE64 encoding and returns it as a string
    """

    def __init__(self, column: AutoMapperColumnOrColumnLikeType):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        column_spec = base64(
            self.column.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            )
        )
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.column
