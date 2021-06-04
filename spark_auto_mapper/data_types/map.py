from typing import Dict, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when

from spark_auto_mapper.type_definitions.defined_types import AutoMapperTextInputType

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)


class AutoMapperMapDataType(AutoMapperDataTypeExpression):
    """
    Applies the supplied mapping to the value of column
    """

    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        mapping: Dict[Optional[AutoMapperTextInputType], AutoMapperAnyDataType],
        default: Optional[AutoMapperAnyDataType] = None,
    ):
        super().__init__(value="")

        self.column: AutoMapperColumnOrColumnLikeType = column
        self.mapping: Dict[AutoMapperAnyDataType, AutoMapperDataTypeBase] = {
            key: (
                value
                if isinstance(value, AutoMapperDataTypeBase)
                else AutoMapperValueParser.parse_value(value)
            )
            for key, value in mapping.items()
        }
        assert self.mapping
        self.default: AutoMapperDataTypeBase = (
            default
            if isinstance(default, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(default)
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        inner_column_spec: Column = self.column.get_column_spec(
            source_df=source_df, current_column=current_column
        )

        column_spec: Optional[Column] = None
        key: AutoMapperAnyDataType
        value: AutoMapperDataTypeBase
        for key, value in self.mapping.items():
            if column_spec is not None:
                column_spec = column_spec.when(
                    inner_column_spec.eqNullSafe(key),  # type: ignore
                    value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                )
            else:
                column_spec = when(
                    inner_column_spec.eqNullSafe(key),  # type: ignore
                    value.get_column_spec(
                        source_df=source_df, current_column=current_column
                    ),
                )

        if column_spec is not None:
            column_spec = column_spec.otherwise(
                self.default.get_column_spec(
                    source_df=source_df, current_column=current_column
                )
            )

        assert column_spec is not None
        return column_spec
