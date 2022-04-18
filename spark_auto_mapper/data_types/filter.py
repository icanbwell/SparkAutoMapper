from typing import Callable, List, Optional, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import filter

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)


class AutoMapperFilterDataType(AutoMapperArrayLikeBase):
    def __init__(
        self,
        column: Union[AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType],
        func: Callable[[Column], Column],
    ) -> None:
        super().__init__()

        self.column: Union[
            AutoMapperDataTypeBase, AutoMapperColumnOrColumnLikeType
        ] = column
        self.func: Callable[[Column], Column] = func

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.column.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        return filter(
            self.column.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ),
            self.func,
        )

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.column
