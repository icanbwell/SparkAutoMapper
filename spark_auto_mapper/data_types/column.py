from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeColumn(AutoMapperDataTypeBase):
    def __init__(self, value: str):
        super().__init__()
        if len(value) > 0 and value[0] == "[":
            self.value: str = value[1:-1]  # skip the first and last characters
        else:
            self.value = value

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if isinstance(self.value, str):
            if not self.value.startswith("a.") and not self.value.startswith(
                "b."
            ):
                # prepend with "b." in case the column exists in both a and b tables
                return col("b." + self.value)
            else:
                return col(self.value)

        raise ValueError(f"value: {self.value} is not str")
