from datetime import date, datetime
from typing import Union

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral

AutoMapperTextType = Union[str, AutoMapperDataTypeLiteral, AutoMapperDataTypeColumn, AutoMapperDataTypeExpression]

AutoMapperSimpleType = Union[str, int, float, date, datetime]

AutoMapperAnyDataType = Union[AutoMapperSimpleType, AutoMapperDataTypeBase]
