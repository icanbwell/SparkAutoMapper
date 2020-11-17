from typing import Any, Dict, Union, TypeVar, cast, Optional

from spark_auto_mapper.data_types.coalesce import AutoMapperCoalesceDataType
from spark_auto_mapper.data_types.hash import AutoMapperHashDataType
from spark_auto_mapper.data_types.if_not_null_or_empty import AutoMapperIfNotNullOrEmptyDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType
from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.complex.complex import AutoMapperDataTypeComplex
from spark_auto_mapper.data_types.concat import AutoMapperConcatDataType
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.date import AutoMapperDateDataType
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.if_not_null import AutoMapperIfNotNullDataType
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.map import AutoMapperMapDataType
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.complex.struct_type import AutoMapperDataTypeStruct
from spark_auto_mapper.data_types.regex_replace import AutoMapperRegExReplaceDataType
from spark_auto_mapper.data_types.substring import AutoMapperSubstringDataType
from spark_auto_mapper.data_types.substring_by_delimiter import AutoMapperSubstringByDelimiterDataType
from spark_auto_mapper.data_types.trim import AutoMapperTrimDataType
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType, AutoMapperBooleanInputType, \
    AutoMapperAmountInputType, AutoMapperNumberInputType, AutoMapperDateInputType
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeTextType, AutoMapperNativeSimpleType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperHelpers:
    @staticmethod
    def struct(value: Dict[str, Any]) -> AutoMapperDataTypeStruct:
        """
        Creates a struct
        :param value: A dictionary to be converted to a struct
        :return: A struct automapper type
        """
        return AutoMapperDataTypeStruct(value=value)

    @staticmethod
    def complex(**kwargs: AutoMapperAnyDataType) -> AutoMapperDataTypeComplex:
        """
        Creates a complex type.
        :param kwargs: parameters to be used to create the complex type
        :return: A complex automapper type
        """
        return AutoMapperDataTypeComplex(**kwargs)

    @staticmethod
    def column(value: str) -> AutoMapperDataTypeColumn:
        """
        Specifies that the value parameter should be used as a column name
        :param value: name of column
        :return: A column automapper type
        """
        return AutoMapperDataTypeColumn(value)

    @staticmethod
    def text(value: str) -> AutoMapperDataTypeLiteral:
        """
        Specifies that the value parameter should be used as a literal text
        :param value: literal text value
        :return: a literal automapper type
        """
        return AutoMapperDataTypeLiteral(value)

    @staticmethod
    def expression(value: str) -> AutoMapperDataTypeExpression:
        """
        Specifies that the value parameter should be executed as a sql expression in Spark
        :param value: sql
        :return: an expression automapper type
        """
        return AutoMapperDataTypeExpression(value)

    @staticmethod
    def date(value: AutoMapperDateInputType) -> AutoMapperDateDataType:
        """
        Specifies that value should be parsed into a date.  We currently support the following formats:
        yyyy-MM-dd
        yyyyMMdd
        MM/dd/yy
        (For adding more, go to AutoMapperDateDataType)
        :param value: text
        :return: a date automapper type
        """
        return AutoMapperDateDataType(value)

    @staticmethod
    def amount(value: AutoMapperAmountInputType) -> AutoMapperAmountDataType:
        """
        Specifies the value should be used as an amount
        :param value:
        :return: an amount automapper type
        """
        return AutoMapperAmountDataType(value)

    @staticmethod
    def boolean(
        value: AutoMapperBooleanInputType
    ) -> AutoMapperBooleanDataType:
        """
        Specifies the value should be used as a boolean
        :param value:
        :return: a boolean automapper type
        """
        return AutoMapperBooleanDataType(value)

    @staticmethod
    def number(value: AutoMapperNumberInputType) -> AutoMapperNumberDataType:
        """
        Specifies value should be used as a number
        :param value:
        :return: a number automapper type
        """
        return AutoMapperNumberDataType(value)

    @staticmethod
    def concat(
        *args: Union[AutoMapperNativeTextType, AutoMapperWrapperType,
                     AutoMapperTextLikeBase]
    ) -> AutoMapperConcatDataType:
        """
        concatenates a list of values.  Each value can be a string or a column
        :param args: string or column
        :return: a concat automapper type
        """
        return AutoMapperConcatDataType(*args)

    @staticmethod
    def if_not_null(
        check: AutoMapperDataTypeColumn,
        value: _TAutoMapperDataType,
        when_null: Optional[Union[AutoMapperTextLikeBase,
                                  _TAutoMapperDataType]] = None
    ) -> _TAutoMapperDataType:
        """
        concatenates a list of values.  Each value can be a string or a column


        :param check: column to check for null
        :param value: what to return if the value is not null
        :param when_null: what value to assign if check is not
        :return: an if_not_null automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullDataType(
                check=check, value=value, when_null=when_null
            )
        )

    @staticmethod
    def if_not_null_or_empty(
        check: AutoMapperDataTypeColumn,
        value: _TAutoMapperDataType,
        when_null_or_empty: Optional[Union[AutoMapperTextLikeBase,
                                           _TAutoMapperDataType]] = None
    ) -> _TAutoMapperDataType:
        """
        concatenates a list of values.  Each value can be a string or a column


        :param check: column to check for null
        :param value: what to return if the value is not null
        :param when_null_or_empty: what value to assign if check is not
        :return: an if_not_null automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullOrEmptyDataType(
                check=check,
                value=value,
                when_null_or_empty=when_null_or_empty
            )
        )

    @staticmethod
    def map(
        column: AutoMapperDataTypeColumn,
        mapping: Dict[str, AutoMapperNativeSimpleType],
        default: Optional[AutoMapperNativeSimpleType] = None
    ) -> AutoMapperDataTypeExpression:
        """
        maps the contents of a column to values
        :param column: column
        :param mapping: A dictionary mapping the contents of the column to other values
                        e.g., {"Y":"Yes", "N": "No"}
        :param default: the value to assign if no value matches
        :return: a map automapper type
        """
        return AutoMapperMapDataType(
            column=column, mapping=mapping, default=default
        )

    @staticmethod
    def left(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        length: int
    ) -> AutoMapperSubstringDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param length: number of characters to take from left
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=0, length=length
        )

    @staticmethod
    def right(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        length: int
    ) -> AutoMapperSubstringDataType:
        """
        Take the specified number of last characters in a string

        :param column: column whose contents to use
        :param length: number of characters to take from right
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=-length, length=length
        )

    @staticmethod
    def substring(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        start: int, length: int
    ) -> AutoMapperSubstringDataType:
        """
        Finds a substring in the specified string.

        :param column: column whose contents to use
        :param start: position to start
        :param length: number of characters to take
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=start, length=length
        )

    @staticmethod
    def string_before_delimiter(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        delimiter: str
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column, delimiter=delimiter, delimiter_count=1
        )

    @staticmethod
    def string_after_delimiter(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        delimiter: str
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column, delimiter=delimiter, delimiter_count=-1
        )

    @staticmethod
    def substring_by_delimiter(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        delimiter: str, delimiter_count: int
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Returns the substring from string str before count occurrences of the delimiter.
        substring_by_delimiter performs a case-sensitive match when searching for delimiter.

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :param delimiter_count: If delimiter_count is positive, everything the left of the final delimiter
                                    (counting from left) is returned.
                                If delimiter_count is negative, every to the right of the final delimiter
                                    (counting from the right) is returned.
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column,
            delimiter=delimiter,
            delimiter_count=delimiter_count
        )

    @staticmethod
    def regex_replace(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase],
        pattern: str, replacement: str
    ) -> AutoMapperRegExReplaceDataType:
        """
        Replace all substrings of the specified string value that match regexp with rep.

        :param column: column whose contents to replace
        :param pattern: pattern to search for
        :param replacement: string to replace with
        :return: a regex_replace automapper type
        """
        return AutoMapperRegExReplaceDataType(
            column=column, pattern=pattern, replacement=replacement
        )

    @staticmethod
    def trim(
        column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase]
    ) -> AutoMapperTrimDataType:
        """
        Trim the spaces from both ends for the specified string column.

        :param column: column whose contents to trim
        :return: a trim automapper type
        """
        return AutoMapperTrimDataType(column=column)

    @staticmethod
    def hash(
        *args: Union[AutoMapperNativeTextType, AutoMapperWrapperType,
                     AutoMapperTextLikeBase]
    ) -> AutoMapperHashDataType:
        """
        Calculates the hash code of given columns, and returns the result as an int column.
        :param args: string or column
        :return: a concat automapper type
        """
        return AutoMapperHashDataType(*args)

    @staticmethod
    def coalesce(*args: _TAutoMapperDataType, ) -> _TAutoMapperDataType:
        """
        Returns the first column that is not null.

        :return: a coalesce automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperCoalesceDataType(*args))
