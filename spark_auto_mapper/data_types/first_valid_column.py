from typing import Generic, Optional, TypeVar, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.utils import AnalysisException

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import (
    AutoMapperColumnOrColumnLikeType,
    AutoMapperAnyDataType,
)

_TAutoMapperDataType = TypeVar("_TAutoMapperDataType", bound=AutoMapperAnyDataType)


class AutoMapperFirstValidColumnType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    Accepts any number of column definitions and will return the first valid column definition, similar to how
    coalesce works, but with the existence of columns rather than null values inside the columns.

    Useful for data sources in which columns may be renamed at some point and you want to process files from before and
    after the name change or when columns are added at a point and are missing from earlier files.
    """

    def __init__(
        self,
        *columns: AutoMapperColumnOrColumnLikeType,
    ):
        super().__init__()

        self.columns: List[AutoMapperColumnOrColumnLikeType] = [
            AutoMapperValueParser.parse_value(column) for column in columns
        ]

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        column_spec = None

        for column in self.columns:
            # noinspection PyBroadException
            try:
                column_spec = column.get_column_spec(
                    source_df=source_df, current_column=current_column
                )
            except Exception:
                # By definition, if we are unable to resolve a column spec, for whatever reason, the column definition
                # is not valid and should try the next column.
                continue

            # noinspection Mypy,PyProtectedMember
            col_name = (
                column_spec._jc.expr().sql()  # type: ignore
            )  # Get spark representation of the column as an expression
            try:
                # Force spark analyzer to confirm that column/expression is possible. This does not actually compute
                # anything, just triggers the analyzer to check validity, which is what we want.
                # If SparkSQL AnalysisException is thrown, continue to next column definition
                if source_df:
                    source_df.selectExpr(col_name.replace("b.", ""))
                    break  # Break as soon as the above query doesn't error as we want the FIRST valid column
            except AnalysisException:
                continue

        assert column_spec is not None
        return column_spec
