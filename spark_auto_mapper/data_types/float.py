from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser


class AutoMapperFloatDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperDataTypeBase):
        super().__init__()
        self.value: AutoMapperDataTypeBase = (
            value
            if isinstance(value, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=value)
        )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        if (
            isinstance(self.value, AutoMapperDataTypeColumn)
            and source_df is not None
            and dict(source_df.dtypes)[self.value.value] not in ("float", "double")
        ):
            # parse the amount here
            column_spec = self.value.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            ).cast("float")
            return column_spec
        else:
            column_spec = self.value.get_column_spec(
                source_df=source_df,
                current_column=current_column,
                parent_columns=parent_columns,
            )
            return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value
