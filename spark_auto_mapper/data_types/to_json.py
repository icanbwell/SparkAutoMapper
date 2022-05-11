from typing import List, Optional, Union

# noinspection PyPackageRequirements
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import to_json

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser


class AutoMapperToJsonDataType(AutoMapperDataTypeBase):
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
        column_spec = self.value.get_column_spec(
            source_df=source_df,
            current_column=current_column,
            parent_columns=parent_columns,
        )
        # if not (
        #     isinstance(self.value, AutoMapperDataTypeColumn)
        #     and source_df
        #     # and dict(source_df.dtypes)[self.value.value] in ["float", "double"]
        # ):
        #     column_spec = to_json(column_spec)
        column_spec = to_json(column_spec)
        return column_spec

    @property
    def children(
        self,
    ) -> Union[AutoMapperDataTypeBase, List[AutoMapperDataTypeBase]]:
        return self.value
