from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import from_unixtime, to_timestamp

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperNumberInputType


class AutoMapperUnixTimestampType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperNumberInputType) -> None:
        """
        Converts the value to a timestamp type in Spark


        :param value: value
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses Spark defaults
        """
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
        # Convert from unix timestamp
        column_spec: Column = to_timestamp(
            from_unixtime(
                self.value.get_column_spec(
                    source_df=source_df,
                    current_column=current_column,
                    parent_columns=parent_columns,
                ),
                format="yyyy-MM-dd HH:mm:ss",
            ),
            format="yyyy-MM-dd HH:mm:ss",
        )

        if source_df is not None:
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
