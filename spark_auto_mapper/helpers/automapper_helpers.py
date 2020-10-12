from datetime import date, datetime
from typing import Union, Dict, Any, Optional

from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_complex import AutoMapperDataTypeComplex
from spark_auto_mapper.data_types.automapper_data_type_date import AutoMapperDateDataType
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_list import AutoMapperDataTypeList
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.automapper_data_type_struct import AutoMapperDataTypeStruct
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


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
    def literal(value: str) -> AutoMapperDataTypeLiteral:
        return AutoMapperDataTypeLiteral(value)

    @staticmethod
    def expression(value: str) -> AutoMapperDataTypeExpression:
        return AutoMapperDataTypeExpression(value)

    @staticmethod
    def date(value: Union[str, date, datetime,
                          AutoMapperDataTypeLiteral, AutoMapperDataTypeColumn, AutoMapperDataTypeExpression]
             ) -> AutoMapperDateDataType:
        return AutoMapperDateDataType(value)

