from datetime import date, datetime
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType


class AutoMapperDataTypeLiteral(AutoMapperTextLikeBase):
    def __init__(
        self, value: Union[AutoMapperNativeSimpleType, AutoMapperTextLikeBase]
    ):
        super().__init__()
        self.value: Union[AutoMapperNativeSimpleType,
                          AutoMapperTextLikeBase] = value

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if not self.value:
            return lit(None)
        if isinstance(self.value, str) or isinstance(self.value, int) \
                or isinstance(self.value, float) or isinstance(self.value, date) \
                or isinstance(self.value, datetime):
            return lit(self.value)
        if isinstance(self.value, AutoMapperTextLikeBase):
            return self.value.get_column_spec(source_df=source_df)

        raise ValueError(f"value: {self.value} is not str")
