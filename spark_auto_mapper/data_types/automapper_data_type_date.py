from datetime import date, datetime
from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import coalesce, to_date

from spark_auto_mapper.helpers.automapper_value_parser import AutoMapperValueParser
from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral


class AutoMapperDataTypeDate(AutoMapperDataTypeBase):
    def __init__(self,
                 value: Union[str, date, datetime,
                              AutoMapperDataTypeLiteral, AutoMapperDataTypeColumn,
                              AutoMapperDataTypeExpression]
                 ):
        super().__init__()
        # keep string separate so we can parse it to date

        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def get_column_spec(self) -> Column:
        if isinstance(self.value, AutoMapperDataTypeColumn) \
                or isinstance(self.value, AutoMapperDataTypeLiteral):
            return coalesce(
                to_date(self.value.get_column_spec(), format='yyyy-MM-dd'),
                to_date(self.value.get_column_spec(), format='yyyyMMdd'),
                to_date(self.value.get_column_spec(), format='MM/dd/yy')
            )
        else:
            column_spec = self.value.get_column_spec()
            return column_spec
