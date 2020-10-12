from typing import Dict, Any, Optional

from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType
from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.complex.complex import AutoMapperDataTypeComplex
from spark_auto_mapper.data_types.date import AutoMapperDateDataType
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.list import AutoMapperDataTypeList
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.complex.struct import AutoMapperDataTypeStruct
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType, AutoMapperBooleanInputType, \
    AutoMapperAmountInputType, AutoMapperNumberInputType, AutoMapperDateInputType


class AutoMapperHelpers:
    @staticmethod
    def list(value: Optional[AutoMapperAnyDataType] = None
             ) -> AutoMapperDataTypeList:
        return AutoMapperDataTypeList(value=value)

    @staticmethod
    def struct(value: Dict[str, Any]) -> AutoMapperDataTypeStruct:
        return AutoMapperDataTypeStruct(value=value)

    @staticmethod
    def complex(**kwargs: AutoMapperAnyDataType) -> AutoMapperDataTypeComplex:
        return AutoMapperDataTypeComplex(**kwargs)

    @staticmethod
    def column(value: str) -> AutoMapperDataTypeColumn:
        return AutoMapperDataTypeColumn(value)

    @staticmethod
    def text(value: str) -> AutoMapperDataTypeLiteral:
        return AutoMapperDataTypeLiteral(value)

    @staticmethod
    def expression(value: str) -> AutoMapperDataTypeExpression:
        return AutoMapperDataTypeExpression(value)

    @staticmethod
    def date(value: AutoMapperDateInputType) -> AutoMapperDateDataType:
        return AutoMapperDateDataType(value)

    @staticmethod
    def amount(value: AutoMapperAmountInputType) -> AutoMapperAmountDataType:
        return AutoMapperAmountDataType(value)

    @staticmethod
    def boolean(value: AutoMapperBooleanInputType) -> AutoMapperBooleanDataType:
        return AutoMapperBooleanDataType(value)

    @staticmethod
    def number(value: AutoMapperNumberInputType) -> AutoMapperNumberDataType:
        return AutoMapperNumberDataType(value)
