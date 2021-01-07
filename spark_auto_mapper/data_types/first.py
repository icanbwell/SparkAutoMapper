from typing import Generic, Optional, TypeVar, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import first

from spark_auto_mapper.data_types.array_base import AutoMapperArrayBase
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperAnyDataType, AutoMapperColumnOrColumnLikeType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=AutoMapperAnyDataType
)


class AutoMapperFirstDataType(
    AutoMapperArrayBase, Generic[_TAutoMapperDataType]
):
    def __init__(
        self, column: Union[AutoMapperArrayBase,
                            AutoMapperColumnOrColumnLikeType]
    ) -> None:
        super().__init__()

        self.column: Union[AutoMapperArrayBase,
                           AutoMapperColumnOrColumnLikeType] = column

    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        column_spec: Column = self.column.get_column_spec(
            source_df=source_df, current_column=current_column
        )

        return first(column_spec)
