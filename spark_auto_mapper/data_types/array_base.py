from typing import Optional, TypeVar, Union, cast, Callable, Dict, Any, List

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

# noinspection PyUnresolvedReferences
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, 'AutoMapperDataTypeBase']
)


class AutoMapperArrayLikeBase(AutoMapperTextLikeBase):
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
    def select(self,
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
    ) -> 'AutoMapperArrayLikeBase':
        """
        filters an array column


        :param func: func to create type or struct
        :return: a filter automapper type
        """
        from spark_auto_mapper.data_types.filter import AutoMapperFilterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            AutoMapperArrayLikeBase,
            AutoMapperFilterDataType(column=self, func=func)
        )

    # noinspection PyMethodMayBeStatic
    def split_by_delimiter(self, delimiter: str) -> 'AutoMapperArrayLikeBase':
        """
        splits a text column by the delimiter to create an array


        :param delimiter: delimiter
        :return: a split_by_delimiter automapper type
        """
        from spark_auto_mapper.data_types.split_by_delimiter import AutoMapperSplitByDelimiterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            AutoMapperArrayLikeBase,
            AutoMapperSplitByDelimiterDataType(
                column=self, delimiter=delimiter
            )
        )

    # noinspection PyMethodMayBeStatic
    def first(self) -> _TAutoMapperDataType:
        """
        returns the first element in array


        :return: a filter automapper type
        """
        from spark_auto_mapper.data_types.first import AutoMapperFirstDataType

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperFirstDataType(column=self))

    # noinspection PyMethodMayBeStatic
    def expression(self, value: str) -> AutoMapperTextLikeBase:
        """
        Specifies that the value parameter should be executed as a sql expression in Spark


        :param value: sql
        :return: an expression automapper type
        """
        from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression

        return AutoMapperDataTypeExpression(value)

    def current(self) -> AutoMapperTextLikeBase:
        """
        Specifies to use the current item
        :return: A column automapper type
        """
        return self.field("_")

    # noinspection PyMethodMayBeStatic
    def field(self, value: str) -> AutoMapperTextLikeBase:
        """
        Specifies that the value parameter should be used as a field name
        :param value: name of column
        :return: A column automapper type
        """
        from spark_auto_mapper.data_types.field import AutoMapperDataTypeField

        return AutoMapperDataTypeField(value)
