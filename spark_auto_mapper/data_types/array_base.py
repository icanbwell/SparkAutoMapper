from typing import Optional, TypeVar, Union, cast, Callable, Dict, Any, List

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
    def transform(self,
                  value: _TAutoMapperDataType) -> List[_TAutoMapperDataType]:
        """
        transforms a column into another type or struct


        :param value: Complex or Simple Type to create for each item in the array
        :return: a transform automapper type
        """
        from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType
        # cast it to the inner type so type checking is happy
        return cast(
            List[_TAutoMapperDataType],
            AutoMapperTransformDataType(column=self, value=value)
        )

    # noinspection PyMethodMayBeStatic
    def filter(
        self, func: Callable[[Dict[str, Any]], Any]
    ) -> _TAutoMapperDataType:
        """
        transforms a column into another type or struct


        :param func: func to create type or struct
        :return: a filter automapper type
        """
        from spark_auto_mapper.data_types.filter import AutoMapperFilterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperFilterDataType(column=self, func=func)
        )

    # noinspection PyMethodMayBeStatic
    def split_by_delimiter(self, delimiter: str) -> 'AutoMapperArrayBase':
        """
        transforms a column into another type or struct


        :param delimiter: delimiter
        :return: a split_by_delimiter automapper type
        """
        from spark_auto_mapper.data_types.split_by_delimiter import AutoMapperSplitByDelimiterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            AutoMapperArrayBase,
            AutoMapperSplitByDelimiterDataType(
                column=self, delimiter=delimiter
            )
        )
