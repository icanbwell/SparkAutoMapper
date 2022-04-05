from typing import Generic, List, Optional, TypeVar, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array
from pyspark.sql.functions import filter
from pyspark.sql.functions import when
from pyspark.sql.types import StructType, ArrayType, StructField, DataType

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_T = TypeVar("_T", bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase])


class AutoMapperNullRemover(AutoMapperDataTypeBase, Generic[_T]):
    """
    Base class for lists
    Generics:  https://mypy.readthedocs.io/en/stable/generics.html
    Multiple Inheritance:
    https://stackoverflow.com/questions/52754339/how-to-express-multiple-inheritance-in-python-type-hint
    """

    def __init__(
        self,
        value: Optional[
            Union[
                List[_T],
                AutoMapperDataTypeBase,
                List[AutoMapperDataTypeBase],
                List[AutoMapperTextLikeBase],
            ]
        ],
        include_null_properties: bool = True,
    ) -> None:
        """
        Generates a list (array) in Spark

        :param value: items to make into an array
        """
        super().__init__()
        # can a single mapper or a list of mappers
        self.value: Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]
        if not value:
            self.value = []
        if isinstance(value, AutoMapperDataTypeBase):
            self.value = value
        elif isinstance(value, List):
            self.value = [AutoMapperValueParser.parse_value(value=v) for v in value]
            # if there are more than two items we have to maintain the same schema in children or Spark errors
            if include_null_properties:
                self.include_null_properties(
                    include_null_properties=include_null_properties
                )
        else:
            raise ValueError(f"{type(value)} is not supported")

    def include_null_properties(self, include_null_properties: bool) -> None:
        if isinstance(self.value, list):
            for item in self.value:
                item.include_null_properties(
                    include_null_properties=include_null_properties
                )
        elif isinstance(self.value, AutoMapperDataTypeBase):
            self.value.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        if isinstance(self.value, list):  # if the src column is a list then iterate
            inner_array = array(
                *[
                    self.get_value(
                        item,
                        source_df=source_df,
                        current_column=current_column,
                        parent_columns=parent_columns,
                    )
                    for item in self.value
                ]
            )
            return when(
                inner_array.isNotNull(),
                filter(inner_array, lambda x: x.isNotNull() & ~x.eqNullSafe("")),
            )

            # if value is an AutoMapper then ask it for its column spec
        if isinstance(self.value, AutoMapperDataTypeBase):
            child: AutoMapperDataTypeBase = self.value
            inner_child_spec = child.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            )
            return when(
                inner_child_spec.isNotNull(),
                filter(inner_child_spec, lambda x: x.isNotNull() & ~x.eqNullSafe("")),
            )

        raise ValueError(f"value: {self.value} is neither list nor AutoMapper")

    # noinspection PyMethodMayBeStatic
    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        if isinstance(self.value, list):
            # get schema for first element
            if len(self.value) > 0:
                first_element = self.value[0]
                schema = first_element.get_schema(
                    include_extension=include_extension,
                    extension_fields=extension_fields,
                )
                if schema is None:
                    return None
                return StructType([StructField("extension", ArrayType(schema))])
        return None

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value
