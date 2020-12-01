from typing import Callable, Generic, TypeVar, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import split

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.spark_higher_order_functions import transform
from spark_auto_mapper.type_definitions.defined_types import AutoMapperTextInputType
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperSplitAndMapDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If check returns null then return null else return value
    """
    def __init__(
        self, check: AutoMapperColumnOrColumnLikeType, delimiter: str,
        func: Callable[[AutoMapperTextInputType], _TAutoMapperDataType]
    ):
        super().__init__()

        self.check: AutoMapperColumnOrColumnLikeType = check
        self.delimiter: str = delimiter
        self.func: Callable[[AutoMapperTextInputType],
                            _TAutoMapperDataType] = func

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = transform(
            split(self.check.get_column_spec(source_df=source_df)), self.func
        )

        return column_spec
