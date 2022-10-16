from typing import Dict, Any, Optional, Union, List, OrderedDict

# noinspection PyPackageRequirements
from pyspark.sql import Column, DataFrame

# noinspection PyPackageRequirements
from pyspark.sql.functions import struct, when, to_json, lit

# noinspection PyPackageRequirements
from pyspark.sql.types import StructType, DataType, StructField

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.field_node import FieldNode
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

        self.remove_empty_structs: bool = kwargs.get("remove_empty_structs", False)
        # self.include_nulls: bool = False

        self.value: Dict[str, AutoMapperDataTypeBase] = {}
        self.set_value_from_kwargs(kwargs)

        self.kwargs: Dict[str, Any] = kwargs

    def set_value_from_kwargs(self, kwargs: Dict[str, Any]) -> None:
        self.value = {
            PythonKeywordCleaner.from_python_safe(
                name=parameter_name
            ): AutoMapperValueParser.parse_value(
                column_name=parameter_name, value=parameter_value
            )
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
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        valid_columns = self.get_child_mappers()
        struct_members = [
            self.get_value(
                value=value,
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ).alias(key)
            for key, value in valid_columns.items()
        ]
        column_spec: Column = (
            when(
                to_json(struct(*struct_members)).eqNullSafe("{}"), lit(None)
            ).otherwise(struct(*struct_members))
            if self.remove_empty_structs
            else struct(*struct_members)
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
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        return None

    def get_fields(self, skip_null_properties: bool) -> List[FieldNode]:
        return list(
            [
                FieldNode(k)
                for k, v in self.value.items()
                if not skip_null_properties
                or (not (isinstance(v, AutoMapperDataTypeLiteral) and v.value is None))
            ]
        )

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return list(self.value.values())

    def filter_schema_by_fields_present(
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

        children: Dict[str, AutoMapperDataTypeBase] = self.value
        name: str
        child: "AutoMapperDataTypeBase"
        for name, child in children.items():
            field_list: List[StructField] = [
                f for f in column_data_type.fields if f.name == name
            ]
            if len(field_list) > 0:
                child.filter_schema_by_fields_present(
                    column_name=field_list[0].name,
                    column_path=f"{column_path}.{field_list[0].name}",
                    column_data_type=field_list[0].dataType,
                    # no need to pass this since it only applies to the first level under a list
                    skip_null_properties=True,
                )

        fields: List[FieldNode] = self.get_fields(
            skip_null_properties=skip_null_properties
        )
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
