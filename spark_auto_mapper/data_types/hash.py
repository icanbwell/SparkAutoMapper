from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import hash

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeTextType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType


class AutoMapperHashDataType(AutoMapperTextLikeBase):
    """
    Calculates the hash code of given columns, and returns the result as an int column.
    """

    def __init__(
        self,
        *args: Union[
            AutoMapperNativeTextType, AutoMapperWrapperType, AutoMapperTextLikeBase
        ]
    ):
        super().__init__()

        self.value: List[AutoMapperDataTypeBase] = [
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=value)
            for value in args
        ]

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        column_spec = hash(
            *[
                col.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                )
                for col in self.value
            ]
        ).cast("string")
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value
