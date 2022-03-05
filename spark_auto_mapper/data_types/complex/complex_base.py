from typing import Dict, Any, Optional, Union, List, OrderedDict

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, DataType

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.python_keyword_cleaner import PythonKeywordCleaner
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeComplexBase(AutoMapperDataTypeBase):
    def __init__(self, **kwargs: Any) -> None:
        """
        base class for complex types
        :param kwargs:
        """
        super().__init__()

        # this flag specifies that we should include all values in the column_spec event NULLs
        self.include_nulls: bool = "include_nulls" in kwargs

        self.value: Dict[str, AutoMapperDataTypeBase] = {}
        self.set_value_from_kwargs(kwargs)

        self.kwargs: Dict[str, Any] = kwargs

    def set_value_from_kwargs(self, kwargs: Dict[str, Any]) -> None:
        self.value = {
            PythonKeywordCleaner.from_python_safe(
                name=parameter_name
            ): AutoMapperValueParser.parse_value(parameter_value)
            for parameter_name, parameter_value in kwargs.items()
        }

    def add_missing_values_and_order(self, expected_keys: List[str]) -> None:
        new_dict: OrderedDict[str, Any] = OrderedDict[str, Any]()
        for expected_key in expected_keys:
            expected_key_safe: str = PythonKeywordCleaner.to_python_safe(expected_key)
            if expected_key_safe in self.kwargs.keys():
                new_dict[expected_key_safe] = self.kwargs[expected_key_safe]
            else:
                new_dict[expected_key_safe] = None
        self.set_value_from_kwargs(new_dict)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.include_nulls = include_null_properties
        # now recursively set this into any other complex children
        for key, value in self.get_child_mappers().items():
            value.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        valid_columns = self.get_child_mappers()
        column_spec: Column = struct(
            *[
                self.get_value(
                    value=value, source_df=source_df, current_column=current_column
                ).alias(key)
                for key, value in valid_columns.items()
            ]
        )
        return column_spec

    def get_child_mappers(self) -> Dict[str, AutoMapperDataTypeBase]:
        valid_columns: Dict[str, AutoMapperDataTypeBase] = {
            key: value
            for key, value in self.value.items()
            if self.include_nulls
            or (
                value is not None
                and not (
                    isinstance(value, AutoMapperDataTypeLiteral) and value.value is None
                )
            )
        }
        return valid_columns

    # override this if your inherited class has a defined schema
    # noinspection PyMethodMayBeStatic
    def get_schema(
        self, include_extension: bool
    ) -> Optional[Union[StructType, DataType]]:
        return None
