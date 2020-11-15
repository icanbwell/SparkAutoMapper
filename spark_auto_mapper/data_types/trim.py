from typing import Union

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import trim
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase


class AutoMapperTrimDataType(AutoMapperTextLikeBase):
    """
    Concatenates multiple strings together
    """
    def __init__(
        self, column: Union[AutoMapperDataTypeColumn, AutoMapperTextLikeBase]
    ):
        super().__init__()

        self.column: Union[AutoMapperDataTypeColumn,
                           AutoMapperTextLikeBase] = column

    def get_column_spec(self, source_df: DataFrame) -> Column:
        column_spec = trim(self.column.get_column_spec(source_df=source_df))
        return column_spec
