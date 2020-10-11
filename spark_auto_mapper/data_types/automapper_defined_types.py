from typing import Union

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.automapper_native_types import AutoMapperSimpleType, AutoMapperNativeListType

AutoMapperTextType = Union[str, AutoMapperDataTypeLiteral, AutoMapperDataTypeColumn, AutoMapperDataTypeExpression]

AutoMapperAnyDataType = Union[AutoMapperSimpleType, AutoMapperNativeListType, AutoMapperDataTypeBase]