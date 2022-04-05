import re
from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame

# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeColumn(AutoMapperArrayLikeBase):
    def __init__(self, value: str):
        super().__init__()
        if len(value) > 0 and value[0] == "[":
            self.value: str = value[1:-1]  # skip the first and last characters
        else:
            self.value = value

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        if isinstance(self.value, str):
            if not self.value.startswith("a.") and not self.value.startswith("b."):
                # prepend with "b." in case the column exists in both a and b tables
                # noinspection RegExpSingleCharAlternation
                elements: List[str] = re.split(r"\.|\[|]", self.value)
                my_column: Optional[Column] = None
                column_default = col("b." + self.value)
                for element in elements:
                    if element != "_" and element != "":
                        element_ = element if not element.isnumeric() else int(element)
                        my_column = (
                            my_column[element_]
                            if my_column is not None
                            else col(f"b.{element_}")
                        )
                return my_column if my_column is not None else column_default
            else:
                return col(self.value)

        raise ValueError(f"value: {self.value} is not str")

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return []
