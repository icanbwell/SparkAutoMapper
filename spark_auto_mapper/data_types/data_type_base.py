from typing import Callable, List, Optional, TypeVar, Union, cast, Type

# noinspection PyPackageRequirements
from pyspark.sql import Column, DataFrame

from typing import TYPE_CHECKING

# noinspection PyPackageRequirements
from pyspark.sql.types import StructType, StringType, DataType

if TYPE_CHECKING:
    from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType
    from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
    from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType
    from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
    from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
    from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
    from spark_auto_mapper.data_types.datetime import AutoMapperDateTimeDataType
    from spark_auto_mapper.data_types.date import AutoMapperDateDataType
    from spark_auto_mapper.data_types.date_format import (
        AutoMapperFormatDateTimeDataType,
    )
    from spark_auto_mapper.data_types.float import AutoMapperFloatDataType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=Union["AutoMapperDataTypeBase"]
)

# used for casting
_TAutoMapperDataType2 = TypeVar(
    "_TAutoMapperDataType2", bound=Union["AutoMapperDataTypeBase"]
)


class AutoMapperDataTypeBase:
    """
    Base class for all Automapper data types
    """

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
        return child.get_column_spec(source_df=source_df, current_column=current_column)

    def include_null_properties(self, include_null_properties: bool) -> None:
        pass  # sub-classes can implement if they support this

    # noinspection PyMethodMayBeStatic
    def transform(
        self: "AutoMapperDataTypeBase", value: _TAutoMapperDataType
    ) -> List[_TAutoMapperDataType]:
        """
        transforms a column into another type or struct


        :param self: Set by Python.  No need to pass.
        :param value: Complex or Simple Type to create for each item in the array
        :return: a transform automapper type
        :example: A.column("last_name").transform(A.complex(bar=A.field("value"), bar2=A.field("system")))
        """
        from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType

        # cast it to the inner type so type checking is happy
        return cast(
            List[_TAutoMapperDataType],
            AutoMapperTransformDataType(column=self, value=value),
        )

    # noinspection PyMethodMayBeStatic
    def select(self, value: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        transforms a column into another type or struct


        :param self: Set by Python.  No need to pass.
        :param value: Complex or Simple Type to create for each item in the array
        :return: a transform automapper type
        :example: A.column("last_name").select(A.complex(bar=A.field("value"), bar2=A.field("system")))
        """
        from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperTransformDataType(column=self, value=value),
        )

    # noinspection PyMethodMayBeStatic
    def filter(
        self: _TAutoMapperDataType, func: Callable[[Column], Column]
    ) -> _TAutoMapperDataType:
        """
        filters an array column


        :param self: Set by Python.  No need to pass.
        :param func: func to create type or struct
        :return: a filter automapper type
        :example: A.column("last_name").filter(lambda x: x["use"] == lit("usual")
        )
        """
        from spark_auto_mapper.data_types.filter import AutoMapperFilterDataType

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType, AutoMapperFilterDataType(column=self, func=func)
        )

    # noinspection PyMethodMayBeStatic
    def split_by_delimiter(
        self: _TAutoMapperDataType, delimiter: str
    ) -> _TAutoMapperDataType:
        """
        splits a text column by the delimiter to create an array


        :param self: Set by Python.  No need to pass.
        :param delimiter: delimiter
        :return: a split_by_delimiter automapper type
        :example: A.column("last_name").split_by_delimiter("|")
        """
        from spark_auto_mapper.data_types.split_by_delimiter import (
            AutoMapperSplitByDelimiterDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperSplitByDelimiterDataType(column=self, delimiter=delimiter),
        )

    def select_one(self, value: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        selects first item from array


        :param self: Set by Python.  No need to pass.
        :param value: Complex or Simple Type to create for each item in the array
        :return: a transform automapper type
        :example: A.column("identifier").select_one(A.field("_.value"))
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
    def first(self: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        returns the first element in array

        :param self: Set by Python.  No need to pass.
        :return: a filter automapper type
        :example: A.column("identifier").select(A.field("_.value")).first()
        """
        from spark_auto_mapper.data_types.first import AutoMapperFirstDataType

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperFirstDataType(column=self))

    # noinspection PyMethodMayBeStatic
    def expression(self: _TAutoMapperDataType, value: str) -> _TAutoMapperDataType:
        """
        Specifies that the value parameter should be executed as a sql expression in Spark

        :param self: Set by Python.  No need to pass.
        :param value: sql to run
        :return: an expression automapper type
        :example: A.column("identifier").expression(
                "
                CASE
                    WHEN `Member Sex` = 'F' THEN 'female'
                    WHEN `Member Sex` = 'M' THEN 'male'
                    ELSE 'other'
                END
                "
                )
        """
        from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression

        return cast(_TAutoMapperDataType, AutoMapperDataTypeExpression(value))

    def current(self) -> _TAutoMapperDataType:
        """
        Specifies to use the current item

        :param self: Set by Python.  No need to pass.
        :return: A column automapper type
        :example: A.column("last_name").current()
        """
        return self.field("_")

    # noinspection PyMethodMayBeStatic
    def field(self, value: str) -> _TAutoMapperDataType:
        """
        Specifies that the value parameter should be used as a field name

        :param self: Set by Python.  No need to pass.
        :param value: name of field
        :return: A column automapper type
        :example: A.column("identifier").select_one(A.field("type.coding[0].code"))
        """
        from spark_auto_mapper.data_types.field import AutoMapperDataTypeField

        return cast(_TAutoMapperDataType, AutoMapperDataTypeField(value))

    # noinspection PyMethodMayBeStatic
    def flatten(self) -> "AutoMapperDataTypeBase":
        """
        creates a single array from an array of arrays.
        If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.
        source: http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html#flatten


        :param self: Set by Python.  No need to pass.
        :return: a flatten automapper type
        :example: A.flatten(A.column("column"))
        """
        from spark_auto_mapper.data_types.flatten import AutoMapperFlattenDataType

        # cast it to the inner type so type checking is happy
        return cast("AutoMapperDataTypeBase", AutoMapperFlattenDataType(column=self))

    # noinspection PyMethodMayBeStatic
    def to_array(self) -> "AutoMapperArrayLikeBase":
        """
        converts single element into an array


        :param self: Set by Python.  No need to pass.
        :return: an automapper type
        :example: A.column("identifier").to_array()
        """
        from spark_auto_mapper.data_types.array import AutoMapperArrayDataType

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperArrayLikeBase",
            AutoMapperArrayDataType(value=self),
        )

    # noinspection PyMethodMayBeStatic
    def concat(
        self: _TAutoMapperDataType, list2: _TAutoMapperDataType
    ) -> _TAutoMapperDataType:
        """
        concatenates two arrays or strings


        :param self: Set by Python.  No need to pass.
        :param list2: list to concat into the current column
        :return: a filter automapper type
        :example: A.column("identifier").concat(A.text("foo").to_array()))
        """
        from spark_auto_mapper.data_types.concat import AutoMapperConcatDataType

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperConcatDataType(self, list2))

    def to_float(self: _TAutoMapperDataType) -> "AutoMapperFloatDataType":
        """
        Converts column to float

        :param self: Set by Python.  No need to pass.
        :return: a float automapper type
        :example: A.column("identifier").to_float()
        """
        from spark_auto_mapper.data_types.float import AutoMapperFloatDataType

        return AutoMapperFloatDataType(value=self)

    def to_date(
        self: _TAutoMapperDataType, formats: Optional[List[str]] = None
    ) -> "AutoMapperDateDataType":
        """
        Converts a value to date only
        For datetime use the datetime mapper type


        :param self: Set by Python.  No need to pass.
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses:
                        y-M-d,
                        yyyyMMdd,
                        M/d/y
        :return: a date type
        :example: A.column("date_of_birth").to_date()
        """
        from spark_auto_mapper.data_types.date import AutoMapperDateDataType

        return AutoMapperDateDataType(self, formats)

    def to_datetime(
        self: _TAutoMapperDataType, formats: Optional[List[str]] = None
    ) -> "AutoMapperDateTimeDataType":
        """
        Converts the value to a timestamp type in Spark


        :param self: Set by Python.  No need to pass.
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses Spark defaults
        :example: A.column("date_of_birth").to_datetime()
        """
        from spark_auto_mapper.data_types.datetime import AutoMapperDateTimeDataType

        return AutoMapperDateTimeDataType(self, formats)

    def to_amount(self: _TAutoMapperDataType) -> "AutoMapperAmountDataType":
        """
        Specifies the value should be used as an amount


        :param self: Set by Python.  No need to pass.
        :return: an amount automapper type
        :example: A.column("payment").to_amount()
        """
        from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType

        return AutoMapperAmountDataType(self)

    def to_boolean(self: _TAutoMapperDataType) -> "AutoMapperBooleanDataType":
        """
        Specifies the value should be used as a boolean


        :param self: Set by Python.  No need to pass.
        :return: a boolean automapper type
        :example: A.column("paid").to_boolean()
        """
        from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType

        return AutoMapperBooleanDataType(self)

    def to_number(self: _TAutoMapperDataType) -> "AutoMapperNumberDataType":
        """
        Specifies value should be used as a number


        :param self: Set by Python.  No need to pass.
        :return: a number automapper type
        :example: A.column("paid").to_number()
        """
        from spark_auto_mapper.data_types.number import AutoMapperNumberDataType

        return AutoMapperNumberDataType(self)

    def to_text(self: _TAutoMapperDataType) -> "AutoMapperTextLikeBase":
        """
        Specifies that the value parameter should be used as a literal text


        :param self: Set by Python.  No need to pass.
        :return: a text automapper type
        :example: A.column("paid").to_text()
        """
        return AutoMapperDataTypeLiteral(self, StringType())

    # noinspection PyMethodMayBeStatic
    def join_using_delimiter(
        self: _TAutoMapperDataType, delimiter: str
    ) -> _TAutoMapperDataType:
        """
        Joins an array and forms a string using the delimiter


        :param self: Set by Python.  No need to pass.
        :param delimiter: string to use as delimiter
        :return: a join_using_delimiter automapper type
        :example: A.column("suffix").join_using_delimiter(", ")
        """
        from spark_auto_mapper.data_types.join_using_delimiter import (
            AutoMapperJoinUsingDelimiterDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperJoinUsingDelimiterDataType(column=self, delimiter=delimiter),
        )

    # override this if your inherited class has a defined schema
    # noinspection PyMethodMayBeStatic
    def get_schema(
        self, include_extension: bool
    ) -> Optional[Union[StructType, DataType]]:
        return None

    def to_date_format(
        self: _TAutoMapperDataType, format_: str
    ) -> "AutoMapperFormatDateTimeDataType":
        """
        Converts a date or time into string


        :param self: Set by Python.  No need to pass.
        :param format_: format to use for trying to parse the value otherwise uses:
                        y-M-d
                        yyyyMMdd
                        M/d/y
        :example: A.column("birth_date").to_date_format("y-M-d")
        """
        from spark_auto_mapper.data_types.date_format import (
            AutoMapperFormatDateTimeDataType,
        )

        return AutoMapperFormatDateTimeDataType(self, format_)

    # noinspection PyMethodMayBeStatic
    def to_null_if_empty(self: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        returns null if the column is an empty string


        :param self: Set by Python.  No need to pass.
        :return: an automapper type
        :example: A.column("my_age").to_null_if_empty()
        """
        from spark_auto_mapper.data_types.null_if_empty import (
            AutoMapperNullIfEmptyDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperNullIfEmptyDataType(value=self))

    def regex_replace(
        self: _TAutoMapperDataType, pattern: str, replacement: str
    ) -> _TAutoMapperDataType:
        """
        Replace all substrings of the specified string value that match regexp with replacement.


        :param self: Set by Python.  No need to pass.
        :param pattern: pattern to search for
        :param replacement: string to replace with
        :return: a regex_replace automapper type
        :example: A.column("last_name").regex_replace("first", "second")
        :example: A.column("last_name").regex_replace(r"[^\r\n\t _.,!\"'/$-]", ".")
        """

        from spark_auto_mapper.data_types.regex_replace import (
            AutoMapperRegExReplaceDataType,
        )

        # cast it to the inner type so type checking is happy
        # noinspection Mypy
        return cast(
            _TAutoMapperDataType,
            AutoMapperRegExReplaceDataType(
                column=self, pattern=pattern, replacement=replacement
            ),
        )

    def sanitize(
        self: _TAutoMapperDataType,
        pattern: str = r"[^\w\r\n\t _.,!\"'/$-]",
        replacement: str = " ",
    ) -> _TAutoMapperDataType:
        """
        Replaces all "non-normal" characters with specified replacement

        By default, We're using the FHIR definition of valid string
        (except /S does not seem to work properly in Spark)
        https://www.hl7.org/fhir/datatypes.html#string
        Valid characters are (regex='[ \r\n\t\\S]'):

        \\S - Any character that is not a whitespace character

           - space

        \r - carriage return

        \n - line feed

        \t - tab


        :param self: Set by Python.  No need to pass.
        :param pattern: regex pattern of characters to replace
        :param replacement: (Optional) string to replace with.  Defaults to space.
        :return: a regex_replace automapper type
        :example: A.column("last_name").sanitize(replacement=".")
        """

        from spark_auto_mapper.data_types.regex_replace import (
            AutoMapperRegExReplaceDataType,
        )

        # cast it to the inner type so type checking is happy
        # noinspection Mypy
        return cast(
            _TAutoMapperDataType,
            AutoMapperRegExReplaceDataType(
                column=self, pattern=pattern, replacement=replacement
            ),
        )

    # noinspection PyMethodMayBeStatic
    def if_exists(
        self: _TAutoMapperDataType,
        if_exists: Optional[_TAutoMapperDataType] = None,
        if_not_exists: Optional[_TAutoMapperDataType] = None,
    ) -> _TAutoMapperDataType:
        """
        returns column if it exists else returns null


        :param self: Set by Python.  No need to pass.
        :param if_exists: value to return if column exists
        :param if_not_exists: value to return if column does not exist
        :return: an automapper type
        :example: A.column("foo").if_exists(A.text("exists"), A.text("not exists"))
        """
        from spark_auto_mapper.data_types.if_column_exists import (
            AutoMapperIfColumnExistsType,
        )

        if not if_exists:
            if_exists = self

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfColumnExistsType(
                column=self, if_exists=if_exists, if_not_exists=if_not_exists
            ),
        )

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def cast(
        self: _TAutoMapperDataType, type_: Type[_TAutoMapperDataType2]
    ) -> _TAutoMapperDataType2:
        """
        casts columns to type


        :param self: Set by Python.  No need to pass.
        :param type_: type to cast to
        :return: an automapper type
        :example: A.column("my_age").cast(AutoMapperNumberDataType)
        """

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType2, self)

    def __add__(
        self: _TAutoMapperDataType, other: _TAutoMapperDataType
    ) -> _TAutoMapperDataType:
        """
        Allows adding items in an array using the + operation


        :param self: Set by Python.  No need to pass.
        :param other: array to add to the current array
        :example: A.column("array1") + [ "foo" ]
        """
        return self.concat(other)

    # noinspection PyMethodMayBeStatic
    def remove_null_or_empty(self) -> "AutoMapperArrayLikeBase":
        """
        converts single element into an array


        :param self: Set by Python.  No need to pass.
        :return: an automapper type
        :example: A.column("identifier").to_array()
        """
        from spark_auto_mapper.data_types.null_remover import AutoMapperNullRemover

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperArrayLikeBase",
            AutoMapperNullRemover(value=self),
        )

    # noinspection PyMethodMayBeStatic
    def if_not_null(
        self: _TAutoMapperDataType,
        value: _TAutoMapperDataType,
        when_null: Optional[_TAutoMapperDataType] = None,
    ) -> _TAutoMapperDataType:
        """
        returns value if the current column is not null else when_null (defaults to null)


        :param self: Set by Python.  No need to pass.
        :param value: value to return if column is not null
        :param when_null: value to return if column is null
        :return: an automapper type
        :example: A.column("foo").if_exists(A.text("exists"), A.text("not exists"))
        """
        from spark_auto_mapper.data_types.if_not_null import (
            AutoMapperIfNotNullDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullDataType(check=self, value=value, when_null=when_null),
        )

    # noinspection PyMethodMayBeStatic
    def if_not_null_or_empty(
        self: _TAutoMapperDataType,
        value: _TAutoMapperDataType,
        when_null_or_empty: Optional[_TAutoMapperDataType] = None,
    ) -> _TAutoMapperDataType:
        """
        returns value if the current column is not null or empty else when_null (defaults to null)


        :param self: Set by Python.  No need to pass.
        :param value: value to return if column is not null
        :param when_null_or_empty: value to return if column is null
        :return: an automapper type
        :example: A.column("foo").if_exists(A.text("exists"), A.text("not exists"))
        """
        from spark_auto_mapper.data_types.if_not_null_or_empty import (
            AutoMapperIfNotNullOrEmptyDataType,
        )

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullOrEmptyDataType(
                check=self, value=value, when_null_or_empty=when_null_or_empty
            ),
        )
