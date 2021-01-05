from typing import Optional, TypeVar, Union, cast

from pyspark.sql import DataFrame, Column
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, 'AutoMapperDataTypeBase']
)


class AutoMapperArrayBase(AutoMapperTextLikeBase):
    # noinspection PyMethodMayBeStatic
    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def transform(self, value: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        transforms a column into another type or struct


        :param value: func to create type or struct
        :return: a transform automapper type
        """
        from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType
        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperTransformDataType(column=self, value=value)
        )
