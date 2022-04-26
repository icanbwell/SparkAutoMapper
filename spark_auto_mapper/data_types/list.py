from typing import Generic, List, Optional, TypeVar, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array, coalesce
from pyspark.sql.functions import lit, filter
from pyspark.sql.functions import when
from pyspark.sql.types import StructType, ArrayType, StructField, DataType
from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_T = TypeVar("_T", bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase])


class AutoMapperList(AutoMapperArrayLikeBase, Generic[_T]):
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
        remove_nulls: bool = True,
        include_null_properties: bool = True,
        children_schema: Optional[Union[StructType, DataType]] = None,
    ) -> None:
        """
        Generates a list (array) in Spark

        :param value: items to make into an array
        :param remove_nulls: whether to remove nulls from the array
        :param children_schema: schema to use for children
        """
        super().__init__()
        if children_schema:
            self.set_children_schema(schema=children_schema)
        # can a single mapper or a list of mappers
        self.remove_nulls: bool = remove_nulls
        self.value: Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]
        if not value:
            self.value = []
        if isinstance(value, str):
            self.value = AutoMapperValueParser.parse_value(value=value)
        elif isinstance(value, AutoMapperDataTypeBase):
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
        self.skip_null_properties: bool = not include_null_properties

    def include_null_properties(self, include_null_properties: bool) -> None:
        """
        Whether to include or exclude null properties from the output

        :param include_null_properties: include if true
        """
        self.skip_null_properties = not include_null_properties
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
        """
        returns a Spark Column definition


        """
        self.ensure_children_have_same_properties(
            skip_null_properties=self.skip_null_properties
        )
        if isinstance(
            self.value, str
        ):  # if the src column is just string then consider it a sql expression
            return array(lit(self.value))

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
                    if item is not None
                ]
            )
            return (
                when(
                    inner_array.isNotNull(),
                    # coalesce is needed otherwise Spark complains:
                    # pyspark.sql.utils.AnalysisException: cannot resolve
                    # 'filter(NULL, lambdafunction((x IS NOT NULL), x))' due to argument data type mismatch:
                    # argument 1 requires array type, however, 'NULL' is of null type.;
                    filter(coalesce(inner_array, array()), lambda x: x.isNotNull()),
                )
                if self.remove_nulls
                else inner_array
            )

        # if value is an AutoMapper then ask it for its column spec
        if isinstance(self.value, AutoMapperDataTypeBase):
            child: AutoMapperDataTypeBase = self.value
            inner_child_spec = child.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            )
            return (
                when(
                    inner_child_spec.isNotNull(),
                    filter(
                        # coalesce is needed otherwise Spark complains:
                        # pyspark.sql.utils.AnalysisException: cannot resolve
                        # 'filter(NULL, lambdafunction((x IS NOT NULL), x))' due to argument data type mismatch:
                        # argument 1 requires array type, however, 'NULL' is of null type.;
                        coalesce(inner_child_spec, array()),
                        lambda x: x.isNotNull(),
                    ),
                )
                if self.remove_nulls
                else inner_child_spec
            )

        raise ValueError(f"value: {self.value} is neither str nor AutoMapper")

    # noinspection PyMethodMayBeStatic
    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[Union[StructType, DataType]]:
        if self.schema:
            return self.schema
        if self.children_schema:
            return self.children_schema
        if isinstance(self.value, list):
            # get schema for first element
            if len(self.value) > 0:
                first_element = self.value[0]
                schema: Optional[
                    Union[StructType, DataType]
                ] = first_element.get_schema(
                    include_extension=include_extension,
                    extension_fields=extension_fields,
                )
                if schema is None:
                    return None
                return StructType([StructField("extension", ArrayType(schema))])
        return None

    def __add__(self, other: "AutoMapperList[_T]") -> "AutoMapperList[_T]":
        """
        This is here to allow using + when adding items of two lists to get a new list


        :param other: other list to append to this one
        """
        new_value: List[AutoMapperDataTypeBase] = []
        if isinstance(self.value, list):
            new_value = new_value + self.value
        else:
            # noinspection PyTypeChecker
            new_value.append(self.value)
        if isinstance(other.value, list):
            new_value = new_value + other.value
        else:
            # noinspection PyTypeChecker
            new_value.append(other.value)

        # iterate through both lists and return a new one
        result: AutoMapperList[_T] = AutoMapperList(
            value=new_value,
            remove_nulls=self.remove_nulls,
            children_schema=self.children_schema,
        )
        return result

    @property
    def children(self) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value

    def check_schema(
        self, parent_column: Optional[str], source_df: Optional[DataFrame]
    ) -> Optional[CheckSchemaResult]:
        # children: List[AutoMapperDataTypeBase]
        # if not isinstance(self.children, list):
        #     children = [self.children]
        # else:
        #     children = self.children

        # child: AutoMapperDataTypeBase
        # for child in children:
        #     result = child.check_schema(parent_column=None, source_df=source_df)
        return None
