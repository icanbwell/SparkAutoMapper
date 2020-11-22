from typing import TypeVar, Union, Generic, Optional, cast

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperIfDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If check returns value if the checks passes else when_not
    """
    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        equals_: _TAutoMapperDataType,
        value: _TAutoMapperDataType,
        else_: Optional[_TAutoMapperDataType] = None
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.check_value: AutoMapperDataTypeBase = equals_ \
            if isinstance(equals_, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(equals_)
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)
        if else_:
            self.else_: AutoMapperDataTypeBase = cast(AutoMapperDataTypeBase, else_) \
                if isinstance(value, AutoMapperDataTypeBase) \
                else AutoMapperValueParser.parse_value(value)
        else:
            self.else_ = AutoMapperDataTypeLiteral(None)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = when(
            self.column.get_column_spec(source_df=source_df).eqNullSafe(
                self.check_value.get_column_spec(source_df=source_df)
            ), self.value.get_column_spec(source_df=source_df)
        ).otherwise(self.else_.get_column_spec(source_df=source_df))

        return column_spec
