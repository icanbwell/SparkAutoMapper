from typing import Generic, Optional, TypeVar

from pyspark.sql import Column, DataFrame
from pyspark.sql.utils import AnalysisException

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=AutoMapperAnyDataType
)


class AutoMapperOptionalType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    Allows for columns to be defined based in which a source column may not exist. If the optional source column does
    not exist, the "default" column definition is used instead.
    """
    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        default: Optional[AutoMapperColumnOrColumnLikeType],
        if_exists: Optional[_TAutoMapperDataType] = None,
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = (
            AutoMapperValueParser.parse_value(column)
        )
        self.default: AutoMapperColumnOrColumnLikeType = (
            AutoMapperValueParser.parse_value(default)
        )

        self.if_exists: Optional[AutoMapperDataTypeBase] = None
        if if_exists:
            self.if_exists = if_exists \
                if isinstance(if_exists, AutoMapperDataTypeBase) \
                else AutoMapperValueParser.parse_value(if_exists)

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = self.column.get_column_spec(
            source_df=source_df, current_column=current_column
        )
        col_name = column_spec._jc.toString(
        )  # Get spark representation of the column
        try:
            # Force spark analyzer to confirm that column/expression is possible. This does not actually compute
            # anything, just triggers the analyzer to check validity, which is what we want.
            # If SparkSQL AnalysisException is thrown, fall-back to the default, otherwise we can proceed.
            if source_df:
                source_df.selectExpr(col_name.replace("b.", ""))
                # col exists so we use the if_exists
                if self.if_exists:
                    column_spec = self.if_exists.get_column_spec(
                        source_df=source_df, current_column=current_column
                    )
        except AnalysisException:
            column_spec = self.default.get_column_spec(
                source_df=source_df, current_column=current_column
            )
        return column_spec
