from typing import Optional, TypeVar, Union, List, Dict

from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType, DataType, StructField

# noinspection PyUnresolvedReferences
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, "AutoMapperDataTypeBase"],
)


class AutoMapperArrayLikeBase(AutoMapperTextLikeBase):
    def __init__(self,
                 children_schema: Optional[Union[StructType, DataType]] = None,
                 ) -> None:
        self.children_schema: Optional[Union[StructType, DataType]] = children_schema
        self.value: Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]

    # noinspection PyMethodMayBeStatic
    def get_column_spec(
            self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        raise NotImplementedError  # base classes should implement this

    def ensure_children_have_same_properties(self) -> None:
        """
        Spark cannot handle children of a list having different properties.
        So we find the superset of properties and add them as null.

        Spark expects the children of a list to have properties in the same order
        So if we have a schema we need to order in that order otherwise just make sure all children have the same order
        """
        if self.value is not None or not isinstance(self.value, list):
            return

        from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase

        children_properties: Dict[AutoMapperDataTypeComplexBase, List[str]] = {
            v: list(v.value.keys())
            for v in self.value
            if isinstance(v, AutoMapperDataTypeComplexBase)
        }
        # find superset of properties and get them in the right order
        superset_of_all_properties: List[str] = []
        for child, child_properties in children_properties.items():
            for child_property in child_properties:
                if child_property not in superset_of_all_properties:
                    superset_of_all_properties.append(child_property)

        ordered_superset_of_all_properties: List[str] = []
        if self.children_schema and isinstance(self.children_schema, StructType):
            field: StructField
            for field in self.children_schema.fields:
                field_name_safe: str = field.name
                if field_name_safe in superset_of_all_properties:
                    ordered_superset_of_all_properties.append(field_name_safe)
            # confirm that there wasn't any field missing from schema
            missing_properties: List[str] = []
            for child_property in superset_of_all_properties:
                if child_property not in ordered_superset_of_all_properties:
                    missing_properties.append(child_property)
            assert len(missing_properties) == 0, (
                f"List had items with properties not present in schema:"
                f" {','.join(missing_properties)}."
                f" list from mappers:{','.join(superset_of_all_properties)}."
                f" list from schema:{','.join(ordered_superset_of_all_properties)}."
            )
        else:
            ordered_superset_of_all_properties = superset_of_all_properties

        for child in [
            v for v in self.value if isinstance(v, AutoMapperDataTypeComplexBase)
        ]:
            child.add_missing_values_and_order(ordered_superset_of_all_properties)

    def set_children_schema(
            self, schema: Optional[Union[StructType, DataType]]
    ) -> None:
        """
        Used by the parent to set the schema for the children of this list

        :param schema: children schema
        """
        self.children_schema = schema
