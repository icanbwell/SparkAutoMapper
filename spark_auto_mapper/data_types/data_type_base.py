from typing import Optional, TypeVar, Union, List, cast, Callable, Dict, Any

from pyspark.sql import Column, DataFrame

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=Union["AutoMapperDataTypeBase"]
)


class AutoMapperDataTypeBase:
    # noinspection PyMethodMayBeStatic
    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        """
        Gets the column spec for this automapper data type

        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        """
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def get_value(
        self,
        value: "AutoMapperDataTypeBase",
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
    ) -> Column:
        """
        Gets the value for this automapper

        :param value: current value
        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        """
        assert isinstance(value, AutoMapperDataTypeBase)
        child: AutoMapperDataTypeBase = value
        return child.get_column_spec(
            source_df=source_df, current_column=current_column
        )

    def include_null_properties(self, include_null_properties: bool) -> None:
        pass  # sub-classes can implement if they support this

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
            AutoMapperTransformDataType(column=self, value=value),
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
            AutoMapperTransformDataType(column=self, value=value),
        )

    # noinspection PyMethodMayBeStatic
    def filter(
        self, func: Callable[[Dict[str, Any]], Any]
    ) -> "AutoMapperDataTypeBase":
        """
        filters an array column


        :param func: func to create type or struct
        :return: a filter automapper type
        """
        from spark_auto_mapper.data_types.filter import AutoMapperFilterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperDataTypeBase",
            AutoMapperFilterDataType(column=self, func=func)
        )

    # noinspection PyMethodMayBeStatic
    def split_by_delimiter(self, delimiter: str) -> "AutoMapperDataTypeBase":
        """
        splits a text column by the delimiter to create an array


        :param delimiter: delimiter
        :return: a split_by_delimiter automapper type
        """
        from spark_auto_mapper.data_types.split_by_delimiter import (
            AutoMapperSplitByDelimiterDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperDataTypeBase",
            AutoMapperSplitByDelimiterDataType(
                column=self, delimiter=delimiter
            ),
        )

    def select_one(self, value: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        selects first item from array


        :param value: Complex or Simple Type to create for each item in the array
        :return: a transform automapper type
        """
        from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType
        from spark_auto_mapper.data_types.first import AutoMapperFirstDataType

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperFirstDataType(
                column=AutoMapperTransformDataType(column=self, value=value)
            ),
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
    def expression(self, value: str) -> _TAutoMapperDataType:
        """
        Specifies that the value parameter should be executed as a sql expression in Spark


        :param value: sql
        :return: an expression automapper type
        """
        from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression

        return cast(_TAutoMapperDataType, AutoMapperDataTypeExpression(value))

    def current(self) -> _TAutoMapperDataType:
        """
        Specifies to use the current item
        :return: A column automapper type
        """
        return self.field("_")

    # noinspection PyMethodMayBeStatic
    def field(self, value: str) -> _TAutoMapperDataType:
        """
        Specifies that the value parameter should be used as a field name
        :param value: name of column
        :return: A column automapper type
        """
        from spark_auto_mapper.data_types.field import AutoMapperDataTypeField

        return cast(_TAutoMapperDataType, AutoMapperDataTypeField(value))

    def float(self) -> "AutoMapperDataTypeBase":
        """
        Converts column to float

        :return:
        :rtype:
        """
        from spark_auto_mapper.data_types.float import AutoMapperFloatDataType

        return cast(
            "AutoMapperDataTypeBase", AutoMapperFloatDataType(value=self)
        )

    # noinspection PyMethodMayBeStatic
    def flatten(self) -> "AutoMapperDataTypeBase":
        """
        creates a single array from an array of arrays.
        If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.
        source: http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html#flatten

        :return: a flatten automapper type
        """
        from spark_auto_mapper.data_types.flatten import AutoMapperFlattenDataType

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperDataTypeBase", AutoMapperFlattenDataType(column=self)
        )
