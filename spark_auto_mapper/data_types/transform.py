from typing import Generic, Optional, TypeVar, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import transform

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperAnyDataType,
    AutoMapperColumnOrColumnLikeType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperTransformDataType(
    AutoMapperArrayLikeBase, Generic[_TAutoMapperDataType]
):
    def __init__(
        self,
        column: Union[AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType],
        value: _TAutoMapperDataType,
    ) -> None:
        super().__init__()

        self.column: Union[
            AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType
        ] = column
        self.value: _TAutoMapperDataType = value

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec: Column = self.column.get_column_spec(
            source_df=source_df, current_column=current_column
        )

        def get_column_spec_for_column(x: Column) -> Column:
            value_get_column_spec: Column = self.value.get_column_spec(
                source_df=source_df, current_column=x
            )
            return value_get_column_spec

        return transform(column_spec, get_column_spec_for_column)
