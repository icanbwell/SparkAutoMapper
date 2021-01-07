from typing import Optional

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, expr

from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase


class AutoMapperDataTypeField(AutoMapperTextLikeBase):
    def __init__(self, value: str):
        super().__init__()
        if len(value) > 0 and value[0] == "[":
            self.value: str = value[1:-1]  # skip the first and last characters
        else:
            self.value = value

    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        if isinstance(self.value, str):
            if self.value.startswith("_") and current_column is not None:
                elements = self.value.split(".")
                if len(elements) > 1:
                    return current_column[elements[1]]
                else:
                    return current_column
            elif "[" in self.value and "]" in self.value:
                return expr(self.value)
            else:
                return col(self.value)

        raise ValueError(f"value: {self.value} is not str")
