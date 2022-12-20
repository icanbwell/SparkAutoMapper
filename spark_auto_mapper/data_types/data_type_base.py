from abc import abstractmethod
from typing import Callable, List, Optional, TypeVar, Union, cast, Type, Dict

# noinspection PyPackageRequirements
from pyspark.sql import Column, DataFrame

from typing import TYPE_CHECKING

# noinspection PyPackageRequirements
from pyspark.sql.types import StructType, StringType, DataType, ArrayType, StructField
from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult
from spark_auto_mapper.helpers.field_node import FieldNode
from spark_auto_mapper.helpers.python_keyword_cleaner import PythonKeywordCleaner

if TYPE_CHECKING:
    from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType
    from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
    from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType
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

    def __init__(self) -> None:
        self.column_name: Optional[str] = None
        self.schema: Optional[Union[StructType, DataType]] = None
        self.children_schema: Optional[Union[StructType, DataType]] = None

    # noinspection PyMethodMayBeStatic
    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        """
        Gets the column spec for this automapper data type

        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        :param parent_columns: Optional[List[Column]]: (Optional) a dictionary of parent columns
                                for use inside nested arrays
        """
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def get_value(
        self,
        value: "AutoMapperDataTypeBase",
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        """
        Gets the value for this automapper

        :param value: current value
        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        :param parent_columns: (Optional) set when inside a nested lambda to access parent columns
        """
        assert isinstance(value, AutoMapperDataTypeBase)
        child: AutoMapperDataTypeBase = value
        return child.get_column_spec(
            source_df=source_df,
            current_column=current_column,
            parent_columns=parent_columns,
        )

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
        :return: A flatten automapper type
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

        from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral

        return AutoMapperDataTypeLiteral(self, StringType())

    def to_string(self: _TAutoMapperDataType) -> "AutoMapperTextLikeBase":
        """
        converts the value to a string type by casting


        :param self: Set by Python.  No need to pass.
        :return: a text automapper type
        :example: A.column("paid").to_text()
        """

        from spark_auto_mapper.data_types.cast_to_type import (
            AutoMapperCastToTypeDataType,
        )
        from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

        return cast(
            AutoMapperTextLikeBase, AutoMapperCastToTypeDataType(self, "string")
        )

    def to_json(self: _TAutoMapperDataType) -> "AutoMapperTextLikeBase":
        """
        converts the value (array or struct) to a string by converting to json


        :param self: Set by Python.  No need to pass.
        :return: a text automapper type
        :example: A.column("paid").to_text()
        """

        from spark_auto_mapper.data_types.to_json import (
            AutoMapperToJsonDataType,
        )
        from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

        return cast(AutoMapperTextLikeBase, AutoMapperToJsonDataType(self))

    def to_type(self: _TAutoMapperDataType, type_: str) -> "AutoMapperDataTypeBase":
        """
        converts the value to the specified type


        :param self: Set by Python.  No need to pass.
        :param type_: type to convert the value to
        :return: a text automapper type
        :example: A.column("paid").to_text()
        """

        from spark_auto_mapper.data_types.cast_to_type import (
            AutoMapperCastToTypeDataType,
        )

        return AutoMapperCastToTypeDataType(self, type_)

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

    # noinspection PyMethodMayBeStatic
    def base64(self: _TAutoMapperDataType) -> "AutoMapperTextLikeBase":
        """
        Computes the BASE64 encoding of the column


        :param self: Set by Python.  No need to pass.
        :return: a base64 automapper type
        :example: A.column("data").base64()
        """
        from spark_auto_mapper.data_types.base64 import (
            AutoMapperBase64DataType,
        )

        return cast(
            AutoMapperTextLikeBase,
            AutoMapperBase64DataType(column=self),
        )

    # override this if your inherited class has a defined schema
    # noinspection PyMethodMayBeStatic
    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
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

    def check_schema(
        self, parent_column: Optional[str], source_df: Optional[DataFrame]
    ) -> Optional[CheckSchemaResult]:
        """
        Checks the schema


        :param parent_column: parent column
        :param source_df: source data frame
        :return: result of checking schema
        """
        return None

    def ensure_children_have_same_properties(self, skip_null_properties: bool) -> None:
        """
        Spark cannot handle children of a list having different properties.
        So we find the superset of properties and add them as null.

        Spark expects the children of a list to have properties in the same order
        So if we have a schema we need to order in that order otherwise just make sure all children have the same order
        """
        if self.children is None or not isinstance(self.children, list):
            return

        for child in self.children:
            assert isinstance(child, AutoMapperDataTypeBase), f"{type(child)}"

        children_properties: Dict[AutoMapperDataTypeBase, List[FieldNode]] = {
            v: v.get_fields(skip_null_properties=skip_null_properties)
            for v in self.children
        }
        # find superset of properties and get them in the right order
        superset_of_all_properties: List[str] = []
        for child, child_properties in children_properties.items():
            for child_property in child_properties:
                if child_property.name not in superset_of_all_properties:
                    superset_of_all_properties.append(child_property.name)

        ordered_superset_of_all_properties: List[str] = []
        if self.children_schema and isinstance(self.children_schema, StructType):
            field: StructField
            for field in self.children_schema.fields:
                field_name_safe: str = field.name
                if field_name_safe in superset_of_all_properties:
                    ordered_superset_of_all_properties.append(field_name_safe)
            # confirm that there wasn't any field missing from schema
            missing_properties: List[str] = []
            for child_property_name in superset_of_all_properties:
                if child_property_name not in ordered_superset_of_all_properties:
                    missing_properties.append(child_property_name)
            assert len(missing_properties) == 0, (
                f"List had items with properties not present in schema:"
                f" {','.join(missing_properties)}."
                f" list from mappers:{','.join(superset_of_all_properties)}."
                f" list from schema:{','.join(ordered_superset_of_all_properties)}."
            )
        else:
            ordered_superset_of_all_properties = superset_of_all_properties

        for child in [v for v in self.children]:
            child.add_missing_values_and_order(ordered_superset_of_all_properties)
            child.include_null_properties(
                True
            )  # must include null properties or will strip the ones we just added

    def set_children_schema(
        self, schema: Optional[Union[StructType, DataType]]
    ) -> None:
        """
        Used by the parent to set the schema for the children of this list

        :param schema: children schema
        """
        self.children_schema = schema

    def get_fields(self, skip_null_properties: bool) -> List[FieldNode]:
        """
        Returns unique list of fields from the children

        """
        fields: List[FieldNode] = []

        children: List[AutoMapperDataTypeBase]
        if not isinstance(self.children, list):
            children = [self.children]
        else:
            children = self.children
        for child in children:
            child_fields: List[FieldNode] = child.get_fields(
                skip_null_properties=skip_null_properties
            )
            for child_field in child_fields:
                if child_field not in fields:
                    fields.append(child_field)
        return fields

    def add_missing_values_and_order(self, expected_keys: List[str]) -> None:
        if not self.children:
            return

        children: List[AutoMapperDataTypeBase]
        if not isinstance(self.children, list):
            children = [self.children]
        else:
            children = self.children
        for child in children:
            child.add_missing_values_and_order(expected_keys=expected_keys)

    def _filter_schema_by_fields_present_for_array(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
        skip_null_properties: bool,
    ) -> DataType:
        assert isinstance(
            column_data_type, ArrayType
        ), f"{type(column_data_type)} should be ArrayType for {column_name} with path {column_path}"

        self.ensure_children_have_same_properties(
            skip_null_properties=skip_null_properties
        )

        element_type = column_data_type.elementType
        children: Union[
            AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]
        ] = self.children
        assert isinstance(children, list), f"{type(children)} should be a list"
        if len(children) > 0:
            should_skip_null_properties: bool = len(children) == 0
            child: AutoMapperDataTypeBase
            for child in children:
                child.filter_schema_by_fields_present(
                    column_name=column_name,
                    column_path=column_path,
                    column_data_type=element_type,
                    skip_null_properties=should_skip_null_properties,
                )

        return column_data_type

    # noinspection PyUnusedLocal
    def _filter_schema_by_fields_present_for_struct(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
        skip_null_properties: bool,
    ) -> DataType:
        assert isinstance(
            column_data_type, StructType
        ), f"{type(column_data_type)} should be StructType for {column_name} with path {column_path}"

        children: Union[
            "AutoMapperDataTypeBase", List["AutoMapperDataTypeBase"]
        ] = self.children
        if isinstance(children, list) and len(children) > 0:
            child: "AutoMapperDataTypeBase"
            for index, child in enumerate(children):
                child.filter_schema_by_fields_present(
                    column_name=column_data_type.fields[index].name,
                    column_path=f"{column_path}.{column_data_type.fields[index].name}",
                    column_data_type=column_data_type.fields[index].dataType,
                    skip_null_properties=skip_null_properties,
                )
        elif not isinstance(children, list) and children is not None:
            child = children
            child.filter_schema_by_fields_present(
                column_name=column_data_type.fields[0].name,
                column_path=f"{column_path}.{column_data_type.fields[0].name}",
                column_data_type=column_data_type.fields[0].dataType,
                skip_null_properties=skip_null_properties,
            )

        fields: List[FieldNode] = self.get_fields(skip_null_properties=True)
        new_column_data_type: DataType = column_data_type
        if isinstance(new_column_data_type, StructType) and len(fields) > 0:
            # return only the values that match the fields
            new_column_data_type.fields = [
                c
                for c in new_column_data_type.fields
                if c.name in [f.name for f in fields] or c.nullable is False
            ]
            new_column_data_type.names = [f.name for f in new_column_data_type.fields]

        return column_data_type

    def filter_schema_by_fields_present(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
        skip_null_properties: bool,
    ) -> DataType:
        # if this is a basic type so nothing to do
        if not isinstance(column_data_type, StructType) and not isinstance(
            column_data_type, ArrayType
        ):
            return column_data_type

        if isinstance(column_data_type, ArrayType):
            return self._filter_schema_by_fields_present_for_array(
                column_name=column_name,
                column_path=column_path,
                column_data_type=column_data_type,
                skip_null_properties=skip_null_properties,
            )

        assert isinstance(
            column_data_type, StructType
        ), f"{type(column_data_type)} should be StructType for {column_name} with path {column_path}"

        return self._filter_schema_by_fields_present_for_struct(
            column_name=column_name,
            column_path=column_path,
            column_data_type=column_data_type,
            skip_null_properties=skip_null_properties,
        )

    def _set_schema_for_array(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
    ) -> None:
        assert isinstance(
            column_data_type, ArrayType
        ), f"{type(column_data_type)} should be ArrayType for {column_name} with path {column_path}"

        element_type = column_data_type.elementType
        self.set_children_schema(element_type)
        children: Union[
            AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]
        ] = self.children
        assert isinstance(children, list), f"{type(children)} should be a list"
        if len(children) > 0:
            child: AutoMapperDataTypeBase
            for child in children:
                child.set_schema(
                    column_name=column_name,
                    column_path=column_path,
                    column_data_type=element_type,
                )

    # noinspection PyUnusedLocal
    def _set_schema_for_struct(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
    ) -> None:
        assert isinstance(
            column_data_type, StructType
        ), f"{type(column_data_type)} should be StructType for {column_name} with path {column_path}"

        children: Union[
            "AutoMapperDataTypeBase", List["AutoMapperDataTypeBase"]
        ] = self.children
        if isinstance(children, list) and len(children) > 0:
            child: "AutoMapperDataTypeBase"
            for index, child in enumerate(children):
                assert isinstance(child, AutoMapperDataTypeBase), f"{type(child)}"
                if not child.column_name:
                    continue
                assert child.column_name, f"No column name for {child}"
                clean_child_name: str = PythonKeywordCleaner.from_python_safe(
                    child.column_name
                )
                matching_fields = [
                    f for f in column_data_type.fields if f.name == clean_child_name
                ]
                assert len(matching_fields) == 1, (
                    f"Schema match failed for column {column_path}.{clean_child_name}"
                    f" in schema fields"
                    f": [{','.join([f.name for f in column_data_type.fields])}]"
                )
                field: StructField = matching_fields[0]
                child.set_schema(
                    column_name=field.name,
                    column_path=f"{column_path}.{field.name}",
                    column_data_type=field.dataType,
                )
        elif not isinstance(children, list) and children is not None:
            child = children
            assert child.column_name
            clean_child_name = PythonKeywordCleaner.from_python_safe(child.column_name)
            matching_fields = [
                f for f in column_data_type.fields if f.name == clean_child_name
            ]
            assert len(matching_fields) == 1, (
                f"Schema match failed for column {column_path}.{clean_child_name}"
                f" in schema fields"
                f": [{','.join([f.name for f in column_data_type.fields])}]"
            )
            field = matching_fields[0]
            child.set_schema(
                column_name=field.name,
                column_path=f"{column_path}.{field.name}",
                column_data_type=field.dataType,
            )

    def set_schema(
        self,
        *,
        column_name: Optional[str],
        column_path: Optional[str],
        column_data_type: DataType,
    ) -> None:
        """
        Sets the schema for this AutoMapper type


        :param column_name: column name
        :param column_path: full path to column
        :param column_data_type: schema for this mapper
        """

        self.schema = column_data_type

        # if this is a basic type so nothing to do
        if not isinstance(column_data_type, StructType) and not isinstance(
            column_data_type, ArrayType
        ):
            return

        if isinstance(column_data_type, ArrayType):
            self._set_schema_for_array(
                column_name=column_name,
                column_path=column_path,
                column_data_type=column_data_type,
            )
            return

        assert isinstance(
            column_data_type, StructType
        ), f"{type(column_data_type)} should be StructType for {column_name} with path {column_path}"

        self._set_schema_for_struct(
            column_name=column_name,
            column_path=column_path,
            column_data_type=column_data_type,
        )

    @property
    @abstractmethod
    def children(
        self,
    ) -> Union["AutoMapperDataTypeBase", List["AutoMapperDataTypeBase"]]:
        """
        The subclasses should implement this
        """
        raise NotImplementedError

    def set_column_name(self, column_name: str) -> None:
        """
        Sets the name for this automapper
        """
        self.column_name = column_name

    def __repr__(self) -> str:
        return (
            f"{self.column_name}: {self.__class__.__name__}"
            if self.column_name is not None
            else self.__class__.__name__
        )
