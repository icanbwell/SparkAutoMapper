from typing import Dict, Optional

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import when, lit

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType


class AutoMapperMapDataType(AutoMapperDataTypeExpression):
    """
    Applies the supplied mapping to the value of column
    """
    def __init__(
        self, column: AutoMapperDataTypeColumn,
        mapping: Dict[str, AutoMapperNativeSimpleType],
        default: Optional[AutoMapperNativeSimpleType]
    ):
        super().__init__(value="")

        self.column: AutoMapperDataTypeColumn = column
        self.mapping: Dict[str, AutoMapperDataTypeBase] = {
            key: (
                value if isinstance(value, AutoMapperDataTypeBase) else
                AutoMapperValueParser.parse_value(value)
            )
            for key, value in mapping.items()
        }
        assert self.mapping
        self.default: Optional[AutoMapperNativeSimpleType] = default

    def get_column_spec(self, source_df: DataFrame) -> Column:
        inner_column_spec: Column = self.column.get_column_spec(
            source_df=source_df
        )

        column_spec = when(
            inner_column_spec.eqNullSafe(list(self.mapping.keys())[0]),
            list(self.mapping.values()
                 )[0].get_column_spec(source_df=source_df)
        )
        key: str
        value: AutoMapperDataTypeBase
        for key, value in list(self.mapping.items())[1:]:
            column_spec = column_spec.when(
                inner_column_spec.eqNullSafe(key),
                value.get_column_spec(source_df=source_df)
            )

        column_spec = column_spec.otherwise(lit(self.default))

        return column_spec
