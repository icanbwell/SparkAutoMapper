from typing import Union

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType, AutoMapperNativeListType

AutoMapperWrapperType = Union[AutoMapperDataTypeLiteral,
                              AutoMapperDataTypeColumn,
                              AutoMapperDataTypeExpression]

AutoMapperAnyDataType = Union[AutoMapperNativeSimpleType,
                              AutoMapperNativeListType, AutoMapperDataTypeBase]

AutoMapperColumnOrColumnLikeType = Union[AutoMapperWrapperType,
                                         AutoMapperTextLikeBase]
