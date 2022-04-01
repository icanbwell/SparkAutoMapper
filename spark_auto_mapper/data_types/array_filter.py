from typing import List, Optional, Union, TypeVar

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import filter, exists

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperArrayFilterDataType(AutoMapperArrayLikeBase):
    def __init__(
        self,
        array_field: AutoMapperColumnOrColumnLikeType,
        inner_array_field: AutoMapperColumnOrColumnLikeType,
        match_property: str,
        match_value: AutoMapperColumnOrColumnLikeType,
    ) -> None:
        super().__init__()

        self.array_field: AutoMapperColumnOrColumnLikeType = array_field
        self.inner_array_field: AutoMapperColumnOrColumnLikeType = inner_array_field
        self.match_property: str = match_property
        self.match_value: AutoMapperColumnOrColumnLikeType = match_value

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.match_value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        #  filter(schedule, exists(schedule, filter(schedule.actor, r -> r.reference == 'Location/unitypoint-421411630')))

        return filter(
            self.array_field.get_column_spec(
                source_df=source_df, current_column=current_column
            ),
            lambda y: exists(
                self.inner_array_field.get_column_spec(
                    source_df=source_df, current_column=current_column
                ),
                lambda x: x[self.match_property]
                == self.match_value.get_column_spec(
                    source_df=source_df, current_column=current_column
                ),
            ),
        )

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.match_value
