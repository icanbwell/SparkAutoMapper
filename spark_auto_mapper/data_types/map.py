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

        column_spec: Optional[Column] = None
        key: str
        value: AutoMapperDataTypeBase
        for key, value in self.mapping.items():
            column_spec = column_spec.when(
                inner_column_spec.eqNullSafe(key),
                value.get_column_spec(source_df=source_df)
            ) if column_spec is not None else when(
                inner_column_spec.eqNullSafe(key),
                value.get_column_spec(source_df=source_df)
            )

        if column_spec is not None:
            column_spec = column_spec.otherwise(lit(self.default))

        return column_spec
