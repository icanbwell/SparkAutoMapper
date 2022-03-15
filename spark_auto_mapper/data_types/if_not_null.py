from typing import TypeVar, Union, Generic, Optional, cast, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase],
)


class AutoMapperIfNotNullDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If check returns null then return null else return value
    """

    def __init__(
        self,
        check: AutoMapperColumnOrColumnLikeType,
        value: _TAutoMapperDataType,
        when_null: Optional[Union[AutoMapperTextLikeBase, _TAutoMapperDataType]] = None,
    ):
        super().__init__()

        self.check: AutoMapperColumnOrColumnLikeType = check
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=value)
        )
        if when_null:
            self.when_null: AutoMapperDataTypeBase = (
                cast(AutoMapperDataTypeBase, when_null)
                if isinstance(value, AutoMapperDataTypeBase)
                else AutoMapperValueParser.parse_value(value=value)
            )
        else:
            self.when_null = AutoMapperDataTypeLiteral(None)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = when(
            self.check.get_column_spec(
                source_df=source_df, current_column=current_column
            ).isNull(),
            self.when_null.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
        ).otherwise(
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
        )

        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return [c for c in [self.value, self.when_null] if c is not None]
