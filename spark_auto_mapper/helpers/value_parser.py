from datetime import date, datetime
from typing import Union, Dict, Any, List

from pyspark.sql.types import DateType, FloatType, IntegerType

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapperValueParser:
    @staticmethod
    def parse_value(
        value: Union[Dict[str, Any], List[Any], AutoMapperAnyDataType],
        include_nulls: bool = False
    ) -> AutoMapperDataTypeBase:
        # convert any short syntax to long syntax
        if isinstance(value, str):
            if len(value) > 0 and value[0] == "[":
                from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
                return AutoMapperDataTypeColumn(
                    value=value[1:-1]
                )  # skip the first and last characters
            else:
                from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
                return AutoMapperDataTypeLiteral(value=value)

        if isinstance(value, int):
            from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(value=value, type_=IntegerType())

        if isinstance(value, float):
            from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(value=value, type_=FloatType())

        if isinstance(value, date):
            from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(value=value, type_=DateType())

        if isinstance(value, datetime):
            from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(value=value, type_=DateType())

        # if value is a dict then wrap with struct
        if isinstance(value, dict):
            from spark_auto_mapper.data_types.complex.struct_type import AutoMapperDataTypeStruct
            return AutoMapperDataTypeStruct(value=value)
        if isinstance(value, List):
            from spark_auto_mapper.data_types.list import AutoMapperList
            return AutoMapperList(value=value)

        if isinstance(value, AutoMapperDataTypeBase):
            return value

        if value is None:
            from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
            return AutoMapperDataTypeLiteral(None)

        raise ValueError(f"{type(value)} is not supported for {value}")
