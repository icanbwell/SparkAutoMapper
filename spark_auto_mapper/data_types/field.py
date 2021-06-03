import re
from typing import Optional, List

from pyspark.sql import Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase


class AutoMapperDataTypeField(AutoMapperArrayLikeBase):
    def __init__(self, value: str):
        super().__init__()
        if len(value) > 0 and value[0] == "[":
            self.value: str = value[1:-1]  # skip the first and last characters
        else:
            self.value = value

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if isinstance(self.value, str):
            if current_column is not None:
                # noinspection RegExpSingleCharAlternation
                elements: List[str] = re.split(r"\.|\[|]", self.value)
                my_column: Column = current_column
                for element in elements:
                    if element != "_" and element != "":
                        my_column = my_column[
                            element if not element.isnumeric() else int(element)
                        ]
                return my_column
            else:
                raise ValueError(
                    ".field() should only be used when iterating over an array"
                )

        raise ValueError(f"value: {self.value} is not str")
