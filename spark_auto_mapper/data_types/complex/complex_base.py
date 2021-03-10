from typing import Dict, Any, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, DataType

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
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

        self.value: Dict[str, AutoMapperDataTypeBase] = {
            parameter_name if not parameter_name.endswith(
                "_"
            )  # some property names are python keywords so we have to append with _
            else parameter_name[:-1]:
            AutoMapperValueParser.parse_value(parameter_value)
            for parameter_name, parameter_value in kwargs.items()
        }

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
                    value=value,
                    source_df=source_df,
                    current_column=current_column
                ).alias(key) for key, value in valid_columns.items()
            ]
        )
        return column_spec

    def get_child_mappers(self) -> Dict[str, AutoMapperDataTypeBase]:
        valid_columns: Dict[str, AutoMapperDataTypeBase] = {
            key: value
            for key, value in self.value.items() if self.include_nulls or (
                value is not None and not (
                    isinstance(value, AutoMapperDataTypeLiteral)
                    and value.value is None
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
