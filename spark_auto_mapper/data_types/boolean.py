from pyspark.sql import Column

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperBooleanInputType


class AutoMapperBooleanDataType(AutoMapperDataTypeBase):
    def __init__(self,
                 value: AutoMapperBooleanInputType
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
