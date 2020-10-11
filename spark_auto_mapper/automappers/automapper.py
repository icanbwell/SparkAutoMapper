from typing import List

from pyspark.sql import DataFrame

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.automapper_columns import AutoMapperColumns
from spark_auto_mapper.automappers.automapper_with_column import AutoMapperWithColumn
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapper(AutoMapperBase):
    def __init__(self, view: str, source_view: str, keys: List[str]):
        super().__init__(parent=None, view=view, source_view=source_view, keys=keys)
        assert view
        assert source_view
        assert keys and len(keys) > 0

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        return df  # this is outer most wrapper so nothing to do

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def withColumn(self,
                   dst_column: str,
                   value: AutoMapperAnyDataType
                   ) -> AutoMapperWithColumn:
        return AutoMapperWithColumn(parent=self, dst_column=dst_column, value=value)

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> AutoMapperColumns:
        return AutoMapperColumns(parent=self, **kwargs)
