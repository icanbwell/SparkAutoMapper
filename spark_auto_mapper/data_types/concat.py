from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import concat

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeTextType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType


class AutoMapperConcatDataType(AutoMapperArrayLikeBase):
    """
    Concatenates multiple strings or arrays together
    """

    def __init__(
        self,
        *args: Union[
            AutoMapperNativeTextType,
            AutoMapperWrapperType,
            AutoMapperTextLikeBase,
            AutoMapperDataTypeBase,
        ]
    ):
        super().__init__()

        self.value: List[AutoMapperDataTypeBase] = [
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=value)
            for value in args
        ]

        # always include null properties in a concat operation
        self.include_null_properties(include_null_properties=True)

    def include_null_properties(self, include_null_properties: bool) -> None:
        for item in self.value:
            item.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        self.ensure_children_have_same_properties(skip_null_properties=False)
        column_spec = concat(
            *[
                col.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                )
                for col in self.value
            ]
        )
        return column_spec

    @property
    def children(self) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        result: List[AutoMapperDataTypeBase] = []
        if self.value is not None:
            for my_value in self.value:
                result.append(my_value)
        return result
