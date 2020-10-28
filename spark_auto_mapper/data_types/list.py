from typing import Union, List, Optional, Generic, TypeVar

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array
from pyspark.sql.functions import lit

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_T = TypeVar(
    "_T", bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperList(AutoMapperDataTypeBase, Generic[_T]):
    """
    Base class for lists
    Generics:  https://mypy.readthedocs.io/en/stable/generics.html
    Multiple Inheritance:
    https://stackoverflow.com/questions/52754339/how-to-express-multiple-inheritance-in-python-type-hint
    """
    def __init__(self, value: Optional[List[_T]]) -> None:
        super().__init__()
        # can a single mapper or a list of mappers
        self.value: Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]
        if not value:
            self.value = []
        if isinstance(value, str):
            self.value = AutoMapperValueParser.parse_value(value=value)
        elif isinstance(value, AutoMapperDataTypeBase):
            self.value = value
        elif isinstance(value, List):
            self.value = [AutoMapperValueParser.parse_value(v) for v in value]
        else:
            raise ValueError(f"{type(value)} is not supported")

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if isinstance(
            self.value, str
        ):  # if the src column is just string then consider it a sql expression
            return array(lit(self.value))

        if isinstance(
            self.value, list
        ):  # if the src column is a list then iterate
            return array(
                [
                    self.get_value(item, source_df=source_df)
                    for item in self.value
                ]
            )

        # if value is an AutoMapper then ask it for its column spec
        if isinstance(self.value, AutoMapperDataTypeBase):
            child: AutoMapperDataTypeBase = self.value
            return array(child.get_column_spec(source_df=source_df))

        raise ValueError(f"value: {self.value} is neither str nor AutoMapper")
