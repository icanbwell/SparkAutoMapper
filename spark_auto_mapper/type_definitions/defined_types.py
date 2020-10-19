from typing import Union

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType, AutoMapperNativeListType, \
    AutoMapperNativeTextType, AutoMapperNativeNumberType, AutoMapperNativeBooleanType, AutoMapperNativeDateType, \
    AutoMapperNativeAmountType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType

AutoMapperTextInputType = Union[AutoMapperNativeTextType,
                                AutoMapperWrapperType]

AutoMapperNumberInputType = Union[AutoMapperNativeNumberType,
                                  AutoMapperWrapperType]

AutoMapperBooleanInputType = Union[AutoMapperNativeBooleanType,
                                   AutoMapperWrapperType]

AutoMapperDateInputType = Union[AutoMapperNativeDateType,
                                AutoMapperWrapperType]

AutoMapperAmountInputType = Union[AutoMapperNativeAmountType,
                                  AutoMapperWrapperType]

AutoMapperAnyDataType = Union[AutoMapperNativeSimpleType,
                              AutoMapperNativeListType, AutoMapperDataTypeBase]
