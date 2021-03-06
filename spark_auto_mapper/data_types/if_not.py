from typing import TypeVar, Union, Generic, Optional, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when, lit

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType, AutoMapperAnyDataType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=AutoMapperAnyDataType
)


class AutoMapperIfNotDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If check returns value if the checks passes else when_not
    """
    def __init__(
        self, column: AutoMapperColumnOrColumnLikeType,
        check: Union[AutoMapperAnyDataType,
                     List[AutoMapperAnyDataType]], value: _TAutoMapperDataType
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        if isinstance(check, list):
            self.check: Union[AutoMapperDataTypeBase,
                              List[AutoMapperDataTypeBase]] = [
                                  a if isinstance(a, AutoMapperDataTypeBase)
                                  else AutoMapperValueParser.parse_value(a)
                                  for a in check
                              ]
        else:
            self.check = check \
                if isinstance(check, AutoMapperDataTypeBase) \
                else AutoMapperValueParser.parse_value(check)
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        if isinstance(self.check, list):
            column_spec = when(
                self.column.get_column_spec(
                    source_df=source_df, current_column=current_column
                ).isin(
                    *[
                        c.get_column_spec(
                            source_df=source_df, current_column=current_column
                        ) for c in self.check
                    ]
                ), lit(None)
            ).otherwise(
                self.value.get_column_spec(
                    source_df=source_df, current_column=current_column
                )
            )
        else:
            column_spec = when(
                self.column.get_column_spec(
                    source_df=source_df, current_column=current_column
                ).eqNullSafe(
                    self.check.get_column_spec(
                        source_df=source_df, current_column=current_column
                    )
                ), lit(None)
            ).otherwise(
                self.value.get_column_spec(
                    source_df=source_df, current_column=current_column
                )
            )

        return column_spec
