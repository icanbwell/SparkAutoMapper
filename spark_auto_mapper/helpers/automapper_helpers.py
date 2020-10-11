from datetime import date, datetime
from typing import Union, List, Dict, Any

from spark_auto_mapper.data_types.automapper_data_type_column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.automapper_data_type_complex import AutoMapperDataTypeComplex
from spark_auto_mapper.data_types.automapper_data_type_complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.data_types.automapper_data_type_date import AutoMapperDataTypeDate
from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.automapper_data_type_list import AutoMapperDataTypeList
from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.automapper_data_type_struct import AutoMapperDataTypeStruct


class AutoMapperHelpers:
    @staticmethod
    def list(value: Union[str, List[str], AutoMapperDataTypeComplexBase] = None
             ) -> AutoMapperDataTypeList:
        return AutoMapperDataTypeList(value=value or [])

    @staticmethod
    def struct(value: Dict[str, Any]) -> AutoMapperDataTypeStruct:
        return AutoMapperDataTypeStruct(value=value)

    @staticmethod
    def complex(**kwargs) -> AutoMapperDataTypeComplex:
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
             ) -> AutoMapperDataTypeDate:
        return AutoMapperDataTypeDate(value)
