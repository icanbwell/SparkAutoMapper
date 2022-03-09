from abc import ABCMeta, abstractmethod
from typing import List, Union, Dict, Optional

from pyspark.sql.types import StructType, StructField, DataType
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class HasChildrenMixin:
    """
    This Mixin provides access to FHIR server configuration
    """

    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        self.children_schema: Optional[Union[StructType, DataType]] = None

    @property
    @abstractmethod
    def children(self) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        """
        The Mixin assumes the class implement this property
        """

    def ensure_children_have_same_properties(self) -> None:
        """
        Spark cannot handle children of a list having different properties.
        So we find the superset of properties and add them as null.

        Spark expects the children of a list to have properties in the same order
        So if we have a schema we need to order in that order otherwise just make sure all children have the same order
        """
        if self.children is None or not isinstance(self.children, list):
            return

        children_properties: Dict[AutoMapperDataTypeBase, List[str]] = {
            v: v.get_fields() for v in self.children
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

        for child in [v for v in self.children]:
            child.add_missing_values_and_order(ordered_superset_of_all_properties)
            child.include_null_properties(
                True
            )  # must include null properties or will strip the ones we jsut added

    def set_children_schema(
        self, schema: Optional[Union[StructType, DataType]]
    ) -> None:
        """
        Used by the parent to set the schema for the children of this list

        :param schema: children schema
        """
        self.children_schema = schema

    def get_fields(self) -> List[str]:
        """
        Returns unique list of fields from the children

        """
        fields: List[str] = []
        children: List[AutoMapperDataTypeBase]
        if not isinstance(self.children, list):
            children = [self.children]
        else:
            children = self.children
        for child in children:
            child_fields: List[str] = child.get_fields()
            for child_field in child_fields:
                if child_field not in fields:
                    fields.append(child_field)
        return fields

    def add_missing_values_and_order(self, expected_keys: List[str]) -> None:
        children: List[AutoMapperDataTypeBase]
        if not isinstance(self.children, list):
            children = [self.children]
        else:
            children = self.children
        for child in children:
            child.add_missing_values_and_order(expected_keys=expected_keys)
