from typing import Dict, Any, Optional, List, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import struct

from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeStruct(AutoMapperDataTypeComplexBase):
    def __init__(self, value: Dict[str, Any]) -> None:
        assert isinstance(value, dict)
        super().__init__(**value)

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        return struct(
            *[
                self.get_value(
                    value=value,
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ).alias(key)
                for key, value in self.value.items()
            ]
        )

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return list(self.value.values())
