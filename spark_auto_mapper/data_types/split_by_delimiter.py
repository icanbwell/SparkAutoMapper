from typing import Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import split

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperSplitByDelimiterDataType(AutoMapperArrayLikeBase):
    """
    Splits the string by the delimiter and returns an array
    """

    def __init__(
        self,
        column: Union[AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType],
        delimiter: str,
    ):
        super().__init__()

        self.column: Union[
            AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType
        ] = column
        # if simple string passed in then convert to regex
        self.delimiter: str = (
            delimiter if delimiter.startswith("[") else f"[{delimiter}]"
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = split(
            self.column.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            self.delimiter,
        )
        return column_spec
