from datetime import date, datetime
from typing import Union, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import DataType

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from spark_auto_mapper.type_definitions.defined_types import AutoMapperTextInputType
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType


class AutoMapperDataTypeLiteral(AutoMapperTextLikeBase):
    def __init__(
        self,
        value: Union[AutoMapperNativeSimpleType, 'AutoMapperTextInputType'],
        type_: Optional[DataType] = None
    ):
        super().__init__()
        self.value: Union[AutoMapperNativeSimpleType,
                          AutoMapperTextInputType] = value
        self.type_: Optional[DataType] = type_

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if self.value is None:
            return lit(None)
        if isinstance(self.value, str) or isinstance(self.value, int) \
                or isinstance(self.value, float) or isinstance(self.value, date) \
                or isinstance(self.value, datetime):
            return lit(self.value).cast(self.type_
                                        ) if self.type_ else lit(self.value)
        if isinstance(self.value, AutoMapperTextLikeBase):
            return self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            ).cast(self.type_) if self.type_ else self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )

        raise ValueError(
            f"value: {self.value} is not str, int,float, date or datetime"
        )
