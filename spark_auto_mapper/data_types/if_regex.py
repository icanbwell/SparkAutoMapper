from typing import Generic, List, Optional, TypeVar, Union, cast

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperIfRegExDataType(AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]):
    """
    If check returns value if the checks passes else when_not
    """

    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        check: Union[str, List[str]],
        value: _TAutoMapperDataType,
        else_: Optional[_TAutoMapperDataType] = None,
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.check: Union[str, List[str]] = check
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=value)
        )
        if else_:
            self.else_: AutoMapperDataTypeBase = (
                cast(AutoMapperDataTypeBase, else_)
                if isinstance(value, AutoMapperDataTypeBase)
                else AutoMapperValueParser.parse_value(value=value)
            )
        else:
            self.else_ = AutoMapperDataTypeLiteral(None)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        # rlike takes a string and not a column
        if isinstance(self.check, list):
            value: str = self.check[0]
            column_spec = when(
                self.column.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ).rlike(value),
                self.value.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ),
            ).otherwise(
                self.else_.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                )
            )
        else:
            value = self.check
            column_spec = when(
                self.column.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ).rlike(value),
                self.value.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ),
            ).otherwise(
                self.else_.get_column_spec(
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
        return [c for c in [self.value, self.else_] if c is not None]
