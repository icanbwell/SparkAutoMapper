from typing import List, Optional

from pyspark.sql import DataFrame


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
