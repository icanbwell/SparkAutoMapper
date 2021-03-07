from typing import TypeVar, Union, Generic, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when, lit

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperNullIfEmptyDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If value returns empty then return null else return value
    """
    def __init__(
        self,
        value: _TAutoMapperDataType,
    ):
        super().__init__()

        self.value: _TAutoMapperDataType = value

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = when(
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ).eqNullSafe(""), lit(None)
        ).otherwise(
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
        )

        return column_spec
