from datetime import date, datetime
from typing import Union, Dict, Any, List

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase


class AutoMapperValueParser:
    @staticmethod
    def parse_value(value: Union[
        str, Dict[str, Any], List[Any], int, float, date, datetime,
        AutoMapperDataTypeBase]
                    ) -> AutoMapperDataTypeBase:
        # convert any short syntax to long syntax
        # if value is a dict then wrap with struct
        if isinstance(value, str):
            if len(value) > 0 and value[0] == "[":
                from spark_auto_mapper.data_types.automapper_data_type_expression import AutoMapperDataTypeExpression
                return AutoMapperDataTypeExpression(value=value[1:-1])  # skip the first and last characters
            else:
                from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
                return AutoMapperDataTypeLiteral(value=value)

        if isinstance(value, int) or isinstance(value, float) or isinstance(value, date) or isinstance(value, datetime):
            from spark_auto_mapper.data_types.automapper_data_type_literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(value=value)

        if isinstance(value, dict):
            from spark_auto_mapper.data_types.automapper_data_type_struct import AutoMapperDataTypeStruct
            return AutoMapperDataTypeStruct(
                value=value
            )
        if isinstance(value, List):
            from spark_auto_mapper.data_types.automapper_data_type_list import AutoMapperDataTypeList
            return AutoMapperDataTypeList(
                value=value
            )

        if isinstance(value, AutoMapperDataTypeBase):
            return value

        raise ValueError(f"{type(value)} is not supported for {value}")
