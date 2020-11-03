from pyspark.sql import DataFrame, Column

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperTextLikeBase(AutoMapperDataTypeBase):
    # noinspection PyMethodMayBeStatic
    def get_column_spec(self, source_df: DataFrame) -> Column:
        raise NotImplementedError  # base classes should implement this
