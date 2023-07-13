from typing import List, Optional, TypeVar, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import filter

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperCrossColumnFilterDataType(AutoMapperArrayLikeBase):
    """
    Filters an array in one column (or an outer array), matching a top level property of that column (or an outer array)
    on a value in a different column (or an inner array).
    """

    def __init__(
        self,
        array_field: AutoMapperColumnOrColumnLikeType,
        match_property: str,
        match_value: AutoMapperColumnOrColumnLikeType,
    ) -> None:
        super().__init__()

        self.array_field: AutoMapperColumnOrColumnLikeType = array_field
        self.match_property: str = match_property
        self.match_value: AutoMapperColumnOrColumnLikeType = match_value

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.match_value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        if parent_columns is None:
            parent_columns = []
        if current_column is not None:
            parent_columns.append(current_column)

        return filter(
            self.array_field.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ),
            lambda x: x[self.match_property]
            == self.match_value.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ),
        )

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.match_value
