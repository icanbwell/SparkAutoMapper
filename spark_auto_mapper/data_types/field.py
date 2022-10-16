import re
from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame

# noinspection PyUnresolvedReferences

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeField(AutoMapperArrayLikeBase):
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
        """ """
        if isinstance(self.value, str):
            if current_column is not None:
                # noinspection RegExpSingleCharAlternation
                elements: List[str] = re.split(r"\.|\[|]", self.value)
                my_column: Column = current_column
                if len(elements) > 1 and "{parent}" in elements:
                    # we want to use the parent column and pop {parent} off the list of elements
                    if parent_columns is not None and len(parent_columns) > 0:
                        elements.remove("{parent}")
                        my_column = parent_columns[-1]
                for element in elements:
                    if element != "_" and element != "":
                        # select the field from column by field name ex: x_0['name']
                        my_column = my_column[
                            element if not element.isnumeric() else int(element)
                        ]
                return my_column
            else:
                raise ValueError(
                    f".field() should only be used when iterating over an array for column {self.column_name}"
                )

        raise ValueError(f"value: {self.value} is not str")

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return []
