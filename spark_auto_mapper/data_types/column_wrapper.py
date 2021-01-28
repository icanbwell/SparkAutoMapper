from typing import Optional

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase


class AutoMapperDataTypeColumnWrapper(AutoMapperTextLikeBase):
    def __init__(self, value: Column):
        super().__init__()
        self.value: Column = value

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        return self.value
