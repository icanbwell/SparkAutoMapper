from typing import Generic, List, Optional, TypeVar, Union

from pyspark.sql import Column, DataFrame
from pyspark.errors import AnalysisException

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperIfColumnExistsType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    Allows for columns to be defined based in which a source column may not exist. If the optional source column does
    not exist, the "default" column definition is used instead.
    """

    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        if_exists: Optional[_TAutoMapperDataType],
        if_not_exists: Optional[_TAutoMapperDataType] = None,
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = (
            AutoMapperValueParser.parse_value(value=column)
        )
        self.if_exists_column: Optional[AutoMapperDataTypeBase] = None
        if if_exists:
            self.if_exists_column = (
                if_exists
                if isinstance(if_exists, AutoMapperDataTypeBase)
                else AutoMapperValueParser.parse_value(value=if_exists)
            )

        self.if_not_exists: Optional[AutoMapperDataTypeBase] = None
        if if_not_exists:
            self.if_not_exists = (
                if_not_exists
                if isinstance(if_not_exists, AutoMapperDataTypeBase)
                else AutoMapperValueParser.parse_value(value=if_not_exists)
            )

    def include_null_properties(self, include_null_properties: bool) -> None:
        if self.if_exists_column is not None:
            self.if_exists_column.include_null_properties(
                include_null_properties=include_null_properties
            )
        if self.if_not_exists is not None:
            self.if_not_exists.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        column_spec = self.column.get_column_spec(
            source_df=source_df,
            current_column=current_column,
            parent_columns=parent_columns,
        )
        try:
            # Force spark analyzer to confirm that column/expression is possible. This does not actually compute
            # anything, just triggers the analyzer to check validity, which is what we want.
            # If SparkSQL AnalysisException is thrown, fall-back to the default, otherwise we can proceed.
            # We use source_df.alias("b").select() so that column refs prefixed with "b." resolve correctly.
            if source_df:
                source_df.alias("b").select(column_spec)
                # col exists so we use the if_exists
                if self.if_exists_column:
                    column_spec = self.if_exists_column.get_column_spec(
                        source_df=source_df,
                        current_column=current_column,
                        parent_columns=parent_columns,
                    )
        except AnalysisException:
            if self.if_not_exists:
                column_spec = self.if_not_exists.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                )
            else:
                column_spec = AutoMapperDataTypeLiteral(None).get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                )
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return [c for c in [self.if_exists_column, self.if_not_exists] if c is not None]
