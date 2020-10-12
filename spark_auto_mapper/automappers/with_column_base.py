from typing import List, Dict

from pyspark.sql import Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser


class AutoMapperWithColumnBase(AutoMapperBase):
    def __init__(self,
                 dst_column: str,
                 value: AutoMapperAnyDataType
                 ) -> None:
        super().__init__()
        # should only have one parameter
        self.dst_column: str = dst_column
        self.value: AutoMapperDataTypeBase = AutoMapperValueParser.parse_value(value) \
            if not isinstance(value, AutoMapperDataTypeBase) \
            else value

    def get_column_spec(self) -> Column:
        # if value is an AutoMapper then ask it for its column spec
        if isinstance(self.value, AutoMapperDataTypeBase):
            child: AutoMapperDataTypeBase = self.value
            column_spec = child.get_column_spec()
            return column_spec.alias(self.dst_column)

        raise ValueError(f"{type(self.value)} is not supported for {self.value}")

    def get_column_specs(self) -> Dict[str, Column]:
        return {self.dst_column: self.get_column_spec()}

    # noinspection PyMethodMayBeStatic
    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        # now add on my stuff
        column_spec: Column = self.get_column_spec()

        conditions = [col(f'b.{key}') == col(f'a.{key}') for key in keys]

        result_df = df.alias('a').join(
            source_df.alias('b'),
            conditions
        ).select(
            [
                col('a.' + xx) for xx in df.columns
            ] + [
                column_spec
            ]
        )
        return result_df
