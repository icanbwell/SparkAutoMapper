from typing import List, Dict, Optional

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructField

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser


class AutoMapperWithColumnBase(AutoMapperBase):
    def __init__(
        self,
        dst_column: str,
        value: AutoMapperAnyDataType,
        column_schema: Optional[StructField],
        include_null_properties: bool,
        skip_if_columns_null_or_empty: Optional[List[str]] = None
    ) -> None:
        super().__init__()
        # should only have one parameter
        self.dst_column: str = dst_column
        self.column_schema: Optional[StructField] = column_schema
        self.value: AutoMapperDataTypeBase = AutoMapperValueParser.parse_value(value) \
            if not isinstance(value, AutoMapperDataTypeBase) \
            else value
        self.skip_if_columns_null_or_empty: Optional[
            List[str]] = skip_if_columns_null_or_empty
        if include_null_properties:
            self.value.include_null_properties(
                include_null_properties=include_null_properties
            )

    def get_column_spec(self, source_df: DataFrame) -> Column:
        # if value is an AutoMapper then ask it for its column spec
        if isinstance(self.value, AutoMapperDataTypeBase):
            child: AutoMapperDataTypeBase = self.value
            column_spec = child.get_column_spec(source_df=source_df)
            if self.skip_if_columns_null_or_empty:
                columns_to_check = f"b.{self.skip_if_columns_null_or_empty[0]}"  # TODO: handle more than one
                # wrap column spec in when
                column_spec = when(
                    col(columns_to_check).isNull()
                    | col(columns_to_check).eqNullSafe(""), lit(None)
                ).otherwise(self.value.get_column_spec(source_df=source_df))
            # if the type has a schema then apply it
            if self.column_schema:
                column_spec = column_spec.cast(self.column_schema.dataType)
            # if dst_column already exists in source_df then prepend with ___ to make it unique
            if self.dst_column in source_df.columns:
                return column_spec.alias(f"___{self.dst_column}")
            else:
                return column_spec.alias(self.dst_column)

        raise ValueError(
            f"{type(self.value)} is not supported for {self.value}"
        )

    def get_column_specs(self, source_df: DataFrame) -> Dict[str, Column]:
        return {self.dst_column: self.get_column_spec(source_df=source_df)}

    # noinspection PyMethodMayBeStatic
    def transform_with_data_frame(
        self, df: DataFrame, source_df: DataFrame, keys: List[str]
    ) -> DataFrame:
        # now add on my stuff
        column_spec: Column = self.get_column_spec(source_df=source_df)
        conditions = [col(f'b.{key}') == col(f'a.{key}') for key in keys]

        existing_columns: List[Column] = [
            col('a.' + column_name) for column_name in df.columns
        ]

        result_df: DataFrame = df.alias('a').join(
            source_df.alias('b'), conditions
        ).select(existing_columns + [column_spec])
        return result_df
