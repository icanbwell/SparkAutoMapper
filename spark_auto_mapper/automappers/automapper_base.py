from typing import List, Optional

from pyspark.sql import DataFrame

from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapperBase:
    def __init__(self,
                 parent: Optional['AutoMapperBase'] = None,
                 view: str = None,
                 source_view: str = None,
                 keys: List[str] = None
                 ) -> None:
        self.parent: Optional[AutoMapperBase] = parent
        self.view: Optional[str] = view
        self.source_view: Optional[str] = source_view
        self.keys: Optional[List[str]] = keys

    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        raise NotImplementedError

    def transform(self, df: DataFrame) -> DataFrame:
        # find the top most AutoMapper which has the information we need
        auto_mapper: Optional[AutoMapperBase] = self
        while auto_mapper and auto_mapper.parent:
            auto_mapper = auto_mapper.parent

        assert auto_mapper
        assert auto_mapper.keys
        source_df: DataFrame = df.sql_ctx.table(auto_mapper.source_view)
        destination_df: DataFrame = source_df.select(auto_mapper.keys)
        return self.transform_with_data_frame(df=destination_df, source_df=source_df, keys=auto_mapper.keys)

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def withColumn(self,
                   dst_column: str,
                   value: AutoMapperAnyDataType
                   ) -> 'AutoMapperBase':
        from spark_auto_mapper.automappers.automapper_with_column import AutoMapperWithColumn
        return AutoMapperWithColumn(parent=self, dst_column=dst_column, value=value)

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> 'AutoMapperBase':
        from spark_auto_mapper.automappers.automapper_columns import AutoMapperColumns
        return AutoMapperColumns(parent=self, **kwargs)
