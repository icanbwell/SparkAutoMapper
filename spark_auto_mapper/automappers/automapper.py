from typing import List, Optional

from pyspark.sql import DataFrame

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.automappers.complex import AutoMapperWithComplex
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.helpers.spark_helpers import SparkHelpers
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapper(AutoMapperContainer):
    def __init__(self, keys: List[str], view: Optional[str] = None, source_view: Optional[str] = None):
        super().__init__()
        assert keys and len(keys) > 0
        self.view: Optional[str] = view
        self.source_view: Optional[str] = source_view
        self.keys: List[str] = keys

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        # iterate over each child mapper and run it
        for column_name, child_mapper in self.mappers.items():
            df = child_mapper.transform_with_data_frame(df=df, source_df=source_df, keys=keys)
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        assert self.keys and len(self.keys) > 0
        # if source_view is specified then load that else assume that df is the source view
        source_df: DataFrame = df.sql_ctx.table(self.source_view) if self.source_view else df
        # if view is specified then check if it exists
        destination_df: DataFrame = df.sql_ctx.table(self.view) \
            if self.view and SparkHelpers.spark_table_exists(sql_ctx=df.sql_ctx, view=self.view) \
            else source_df.select(self.keys)
        # run the mapper
        result_df: DataFrame = self.transform_with_data_frame(df=destination_df, source_df=source_df, keys=self.keys)
        # if view was specified then create that view
        if self.view:
            result_df.createOrReplaceTempView(self.view)
        return result_df

    def register_child(self,
                       dst_column: str,
                       child: 'AutoMapperBase'
                       ) -> None:
        self.mappers[dst_column] = child

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> 'AutoMapper':
        from spark_auto_mapper.automappers.columns import AutoMapperColumns
        columns_mapper: AutoMapperColumns = AutoMapperColumns(**kwargs)
        for column_name, child_mapper in columns_mapper.mappers.items():
            self.register_child(dst_column=column_name, child=child_mapper)
        return self

    # noinspection PyPep8Naming,PyMethodMayBeStatic
    def complex(self,
                entity: AutoMapperDataTypeComplexBase
                ) -> 'AutoMapper':
        resource_mapper: AutoMapperWithComplex = AutoMapperWithComplex(entity=entity)
        for column_name, child_mapper in resource_mapper.mappers.items():
            self.register_child(dst_column=column_name, child=child_mapper)
        return self
