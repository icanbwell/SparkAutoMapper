from typing import Union

from pyspark.sql import Column

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.automapper_value_parser import AutoMapperValueParser


class AutoMapperNumberDataType(AutoMapperDataTypeBase):
    def __init__(self,
                 value: Union[str, float,
                              AutoMapperDataTypeLiteral, AutoMapperDataTypeColumn,
                              AutoMapperDataTypeExpression]
                 ):
        super().__init__()
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def get_column_spec(self) -> Column:
        if isinstance(self.value, AutoMapperDataTypeColumn) \
                or isinstance(self.value, AutoMapperDataTypeLiteral):
            # parse the amount here
            column_spec = self.value.get_column_spec()
            return column_spec
        else:
            column_spec = self.value.get_column_spec()
            return column_spec
