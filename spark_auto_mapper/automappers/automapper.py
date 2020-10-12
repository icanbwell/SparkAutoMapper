from typing import List

from pyspark.sql import DataFrame

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.automapper_container import AutoMapperContainer
from spark_auto_mapper.automappers.automapper_with_complex import AutoMapperWithComplex
from spark_auto_mapper.data_types.automapper_data_type_complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapper(AutoMapperContainer):
    def __init__(self, view: str, source_view: str, keys: List[str]):
        super().__init__()
        assert view
        assert source_view
        assert keys and len(keys) > 0
        self.view: str = view
        self.source_view: str = source_view
        self.keys: List[str] = keys

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        # iterate over each child mapper and run it
        for column_name, child_mapper in self.mappers.items():
            df = child_mapper.transform_with_data_frame(df=df, source_df=source_df, keys=keys)
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        assert self.view
        assert self.source_view
        assert self.keys
        source_df: DataFrame = df.sql_ctx.table(self.source_view)
        destination_df: DataFrame = source_df.select(self.keys)
        return self.transform_with_data_frame(df=destination_df, source_df=source_df, keys=self.keys)

    def register_child(self,
                       dst_column: str,
                       child: 'AutoMapperBase'
                       ) -> None:
        self.mappers[dst_column] = child

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def withColumn(self,
                   **kwargs: AutoMapperAnyDataType
                   ) -> 'AutoMapper':
        from spark_auto_mapper.automappers.automapper_with_column import AutoMapperWithColumn
        with_column_mapper: AutoMapperWithColumn = AutoMapperWithColumn(**kwargs)
        self.register_child(dst_column=with_column_mapper.dst_column, child=with_column_mapper)
        return self

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> 'AutoMapper':
        from spark_auto_mapper.automappers.automapper_columns import AutoMapperColumns
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
