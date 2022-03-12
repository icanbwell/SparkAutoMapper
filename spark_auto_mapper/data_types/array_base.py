from typing import Optional, TypeVar, Union, List

from pyspark.sql import DataFrame, Column

# noinspection PyUnresolvedReferences
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, "AutoMapperDataTypeBase"],
)


class AutoMapperArrayLikeBase(AutoMapperTextLikeBase):
    # noinspection PyMethodMayBeStatic
    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        raise NotImplementedError  # base classes should implement this

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return []
