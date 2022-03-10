from typing import TypeVar, Union, Generic, Optional, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when, lit

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase],
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
        if isinstance(self.value, AutoMapperDataTypeBase):
            self.value.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        value_spec = (
            self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            if isinstance(self.value, AutoMapperDataTypeBase)
            else lit(self.value)
        )
        column_spec = when(
            value_spec.eqNullSafe(""),
            lit(None),
        ).otherwise(value_spec)

        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value  # type: ignore
