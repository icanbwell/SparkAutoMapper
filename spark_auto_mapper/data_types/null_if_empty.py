from typing import Generic, List, Optional, TypeVar, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when, lit, to_json, size

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.list import AutoMapperList
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
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        value_spec = (
            self.value.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            )
            if isinstance(self.value, AutoMapperDataTypeBase)
            else lit(self.value)
        )
        from spark_auto_mapper.data_types.complex.complex_base import (
            AutoMapperDataTypeComplexBase,
        )

        column_spec = when(
            # if struct then check if it is empty
            (value_spec.isNull()) | (to_json(value_spec).eqNullSafe("{}"))
            if isinstance(self.value, AutoMapperDataTypeComplexBase)
            # if array then check if it is empty
            else (value_spec.isNull()) | (size(value_spec) == 0)
            if isinstance(self.value, AutoMapperList)
            # else check if string is empty
            else value_spec.eqNullSafe(""),
            lit(None),
        ).otherwise(value_spec)

        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value  # type: ignore
