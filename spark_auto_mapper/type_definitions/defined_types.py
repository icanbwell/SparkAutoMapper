from typing import Union

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.type_definitions.native_types import (
    AutoMapperNativeSimpleType,
    AutoMapperNativeListType,
    AutoMapperNativeTextType,
    AutoMapperNativeNumberType,
    AutoMapperNativeBooleanType,
    AutoMapperNativeDateType,
    AutoMapperNativeAmountType,
)
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
)

AutoMapperTextInputType = Union[
    AutoMapperNativeTextType, AutoMapperColumnOrColumnLikeType
]

AutoMapperNumberInputType = Union[
    AutoMapperNativeNumberType, AutoMapperColumnOrColumnLikeType
]

AutoMapperBooleanInputType = Union[
    AutoMapperNativeBooleanType, AutoMapperColumnOrColumnLikeType
]

AutoMapperDateInputType = Union[
    AutoMapperNativeDateType, AutoMapperColumnOrColumnLikeType
]

AutoMapperAmountInputType = Union[
    AutoMapperNativeAmountType, AutoMapperColumnOrColumnLikeType
]

AutoMapperAnyDataType = Union[
    AutoMapperNativeSimpleType, AutoMapperNativeListType, AutoMapperDataTypeBase
]

AutoMapperString = Union[AutoMapperTextInputType, AutoMapperTextLikeBase]
