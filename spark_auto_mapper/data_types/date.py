from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, to_date

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperDateInputType


class AutoMapperDateDataType(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperDateInputType):
        super().__init__()
        # keep string separate so we can parse it to date

        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if source_df is not None and isinstance(self.value, AutoMapperDataTypeColumn) \
                and not dict(source_df.dtypes)[self.value.value] == "date":
            return coalesce(
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='y-M-d'
                ),
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='yyyyMMdd'
                ),
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='M/d/y'
                )
            )
        elif isinstance(self.value, AutoMapperDataTypeLiteral):
            return coalesce(
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='y-M-d'
                ),
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='yyyyMMdd'
                ),
                to_date(
                    self.value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                    format='M/d/y'
                )
            )
        else:
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            return column_spec
