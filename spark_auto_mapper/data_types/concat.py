from typing import List, Union, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import concat

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeTextType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType


class AutoMapperConcatDataType(AutoMapperTextLikeBase):
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
            else AutoMapperValueParser.parse_value(value)
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
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = concat(
            *[
                col.get_column_spec(source_df=source_df, current_column=current_column)
                for col in self.value
            ]
        )
        return column_spec
