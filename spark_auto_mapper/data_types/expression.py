from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import expr

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeExpression(AutoMapperDataTypeBase):
    def __init__(self, value: str):
        super().__init__()
        self.value: str = value

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if isinstance(
            self.value, str
        ):  # if the src column is just string then consider it a sql expression
            return expr(self.value)

        raise ValueError(f"value: {self.value} is not str")
